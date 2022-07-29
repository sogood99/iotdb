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
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MigrateManager {
  private static final Logger logger = LoggerFactory.getLogger(MigrateManager.class);
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  protected static StorageEngine storageEngine = StorageEngine.getInstance();
  private MigrateLogWriter logWriter;
  private List<MigrateTask> migrateTasks = new ArrayList<>();
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
      while (logReader.hasNext()) {
        MigrateLogWriter.MigrateLog log = logReader.next();
        // parse cmd
        logger.info("migrate read:" + log.index + " " + log.storageGroup);
        setMigrateFromLog(
            log.index,
            log.storageGroup,
            FSFactoryProducer.getFSFactory().getFile(log.targetDirPath),
            log.ttl,
            log.startTime);
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
  }

  /** creates a copy of migrateTasks and returns */
  public List<MigrateTask> getMigrateTasks() {
    return new ArrayList<>(migrateTasks);
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
    migrateTasks.add(newTask);
    currentTaskIndex++;
  }

  /** add migration task to migrationTasks from log, does not write to log */
  public void setMigrateFromLog(
      long index, PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    if (currentTaskIndex > index) {
      logger.error("set migrate error, current index larger than log index");
    }

    MigrateTask newTask = new MigrateTask(index, storageGroup, targetDir, ttl, startTime);
    migrateTasks.add(newTask);
    currentTaskIndex = index;
  }

  /**
   * remove migration task from migrationTasks list using index
   *
   * @param removeIndex index of task to remove
   * @return true if task with index exists
   */
  public boolean removeMigrate(long removeIndex) {
    for (int i = 0; i < migrateTasks.size(); i++) {
      if (migrateTasks.get(i).getIndex() == removeIndex) {
        migrateTasks.remove(i);
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
  public boolean removeMigrate(PartialPath storageGroup) {
    for (int i = 0; i < migrateTasks.size(); i++) {
      if (migrateTasks.get(i).getStorageGroup() == storageGroup) {
        migrateTasks.remove(i);
        return true;
      }
    }
    return false;
  }

  /** check if any of the migrateTasks can start */
  public synchronized void checkMigrate() {
    // copy migrateTasks to allow deletion
    List<MigrateTask> migrateTaskList = getMigrateTasks();

    for (int i = 0; i < migrateTaskList.size(); i++) {
      MigrateTask task = migrateTaskList.get(i);

      if (task.getStartTime() - DatetimeUtils.currentTime() <= 0
          && task.getStatus() == MigrateTask.MigrateTaskStatus.READY) {

        // storage group has no data
        if (!storageEngine.getProcessorMap().containsKey(task.getStorageGroup())) {
          return;
        }

        // set task to migrating
        migrateTasks.get(i).setStatus(MigrateTask.MigrateTaskStatus.RUNNING);

        // push check migration to storageGroupManager, use future to give task to thread pool
        migrateTaskThreadPool.submit(
            () -> {
              storageEngine
                  .getProcessorMap()
                  .get(task.getStorageGroup())
                  .checkMigrate(task.getTargetDir(), task.getTTL());
              logger.info("check migration task successfully.");

              // set state and remove
              migrateTasks.remove(task);
            });
      }
    }
  }
}
