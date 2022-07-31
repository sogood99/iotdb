/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.migrate;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MigrateManager {
  private static final Logger logger = LoggerFactory.getLogger(MigrateManager.class);
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private MigrateLogWriter logWriter;

  // index -> MigrateTask
  private ConcurrentHashMap<Long, MigrateTask> migrateTasks = new ConcurrentHashMap<>();
  private long currentTaskIndex = 0;
  private ScheduledExecutorService migrateCheckThread;
  private ExecutorService migrateTaskThreadPool;

  private boolean initialized = false;
  private static final long MIGRATE_CHECK_INTERVAL = 60 * 1000L;
  private static final String LOG_FILE_NAME =
      FilePathUtils.regularizePath(config.getSystemDir())
          + File.separator
          + "migration"
          + File.separator
          + "log.bin";

  protected MigrateManager() {
    init();
  }

  // singleton
  private static class MigrateManagerHolder {
    private MigrateManagerHolder() {}

    private static final MigrateManager INSTANCE = new MigrateManager();
  }

  public static MigrateManager getInstance() {
    return MigrateManagerHolder.INSTANCE;
  }

  public synchronized void init() {
    if (initialized) {
      return;
    }

    try {
      logWriter = new MigrateLogWriter(LOG_FILE_NAME);
    } catch (FileNotFoundException e) {
      logger.error("Cannot find/create log for migrate.");
    }

    // read from logReader
    try {
      MigrateLogReader logReader = new MigrateLogReader(LOG_FILE_NAME);
      Set<Long> errorSet = new HashSet<>();

      while (logReader.hasNext()) {
        MigrateLogWriter.MigrateLog log = logReader.next();

        switch (log.type) {
          case SET:
            setMigrateFromLog(
                log.index,
                log.storageGroup,
                FSFactoryProducer.getFSFactory().getFile(log.targetDirPath),
                log.ttl,
                log.startTime);
            break;
          case UNSET:
            unsetMigrateFromLog(log.index);
            break;
          case START:
            // if task started but didn't finish, then error occurred
            errorSet.add(log.index);
            break;
          case FINISHED:
            // finished task, remove from potential error task
            errorSet.remove(log.index);
            finishFromLog(log.index);
            break;
          case ERROR:
            // already put error in log
            errorSet.remove(log.index);
            errorFromLog(log.index);
            break;
          default:
            logger.error("read migrate log: unknown type");
        }
      }

      // for each task in errorSet, the task started but didn't finish (an error)
      for (long errIndex : errorSet) {
        if (migrateTasks.containsKey(errIndex)) {
          // write to log and set task in ERROR in memory
          logWriter.error(migrateTasks.get(errIndex));
          errorFromLog(errIndex);
        } else {
          logger.error("unknown error index");
        }
      }

    } catch (IOException e) {
      logger.error("Cannot read log for migrate.");
    }

    migrateCheckThread = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Migration-Check");
    migrateCheckThread.scheduleAtFixedRate(
        this::checkMigrate, MIGRATE_CHECK_INTERVAL, MIGRATE_CHECK_INTERVAL, TimeUnit.MILLISECONDS);

    migrateTaskThreadPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(config.getMigrateThreadCount(), "Migration-Task");
    logger.info("start migrate check thread successfully.");
    initialized = true;
  }

  /** creates a copy of migrateTasks and returns */
  public ConcurrentHashMap<Long, MigrateTask> getMigrateTasks() {
    return new ConcurrentHashMap<>(migrateTasks);
  }

  /** add migration task to migrationTasks */
  public void setMigrate(PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    MigrateTask newTask =
        new MigrateTask(currentTaskIndex, storageGroup, targetDir, ttl, startTime);
    try {
      logWriter.setMigrate(newTask);
    } catch (IOException e) {
      logger.error("write log error");
      return;
    }
    migrateTasks.put(currentTaskIndex, newTask);
    currentTaskIndex++;
  }

  /** add migration task to migrationTasks from log, does not write to log */
  public void setMigrateFromLog(
      long index, PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    if (currentTaskIndex > index) {
      logger.error("set migrate error, current index larger than log index");
    }

    MigrateTask newTask = new MigrateTask(index, storageGroup, targetDir, ttl, startTime);
    migrateTasks.put(index, newTask);
    currentTaskIndex = index;
  }

  /**
   * remove migration task from migrationTasks list using index
   *
   * @param removeIndex index of task to remove
   * @return true if task with index exists
   */
  public boolean unsetMigrate(long removeIndex) {
    if (migrateTasks.containsKey(removeIndex)) {
      MigrateTask task = migrateTasks.get(removeIndex);
      if (task.getIndex() == removeIndex
          && (task.getStatus() == MigrateTask.MigrateTaskStatus.READY
              || task.getStatus() == MigrateTask.MigrateTaskStatus.RUNNING)) {
        // write to log
        try {
          logWriter.unsetMigrate(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to unset
        task.setStatus(MigrateTask.MigrateTaskStatus.UNSET);
        return true;
      }
    }
    return false;
  }

  /**
   * remove migration task from migrationTasks list using storage group if multiple tasks with such
   * storage group exists, remove the one with the lowest index
   *
   * @param storageGroup sg name for task to remove
   * @return true if exists task with storageGroup
   */
  public boolean unsetMigrate(PartialPath storageGroup) {
    for (MigrateTask task : migrateTasks.values()) {
      if (task.getStorageGroup() == storageGroup
          && (task.getStatus() == MigrateTask.MigrateTaskStatus.READY
              || task.getStatus() == MigrateTask.MigrateTaskStatus.RUNNING)) {
        // write to log
        try {
          logWriter.unsetMigrate(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to UNSET
        task.setStatus(MigrateTask.MigrateTaskStatus.UNSET);
        return true;
      }
    }
    return false;
  }

  /** same as unsetMigrate using index except does not write to log */
  public boolean unsetMigrateFromLog(long index) {
    if (migrateTasks.containsKey(index)) {
      migrateTasks.remove(index);
      return true;
    } else {
      return false;
    }
  }

  /** set to finish status */
  public boolean finishFromLog(long index) {
    if (migrateTasks.containsKey(index)) {
      migrateTasks.get(index).setStatus(MigrateTask.MigrateTaskStatus.FINISHED);
      return true;
    } else {
      return false;
    }
  }

  /** set to error status */
  public boolean errorFromLog(long index) {
    if (migrateTasks.containsKey(index)) {
      migrateTasks.get(index).setStatus(MigrateTask.MigrateTaskStatus.ERROR);
      return true;
    } else {
      return false;
    }
  }

  /** check if any of the migrateTasks can start */
  public synchronized void checkMigrate() {
    logger.info("check migration");
    for (MigrateTask task : migrateTasks.values()) {

      if (task.getStartTime() - DatetimeUtils.currentTime() <= 0
          && task.getStatus() == MigrateTask.MigrateTaskStatus.READY) {

        // storage group has no data
        if (!StorageEngine.getInstance().getProcessorMap().containsKey(task.getStorageGroup())) {
          return;
        }

        // set task to migrating
        task.setStatus(MigrateTask.MigrateTaskStatus.RUNNING);

        // push check migration to storageGroupManager, use future to give task to thread pool
        migrateTaskThreadPool.execute(
            () -> {
              try {
                logWriter.startMigrate(task);
              } catch (IOException e) {
                logger.error("write log error");
              }

              StorageEngine.getInstance()
                  .getProcessorMap()
                  .get(task.getStorageGroup())
                  .checkMigrate(task.getTargetDir(), task.getTTL());
              logger.info("check migration task successfully.");

              // set state and remove
              try {
                logWriter.finishMigrate(task);
              } catch (IOException e) {
                logger.error("write log error");
              }
              task.setStatus(MigrateTask.MigrateTaskStatus.FINISHED);
            });
      }
    }
  }
}
