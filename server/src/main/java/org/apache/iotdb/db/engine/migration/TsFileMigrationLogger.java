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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.metadata.idtable.IDTable.config;
import static org.apache.iotdb.db.metadata.idtable.IDTable.logger;

/**
 * To assure that migration of tsFiles is pesudo-atomic operator, TsFileMigrationLogger writes files
 * to migratingFileDir when a migration task migrates a tsFile (and its resource/mod files) , then
 * deletes it after the task has finished operation.
 */
public class TsFileMigrationLogger implements AutoCloseable {

  // taskId -> MigratingFileLog
  ConcurrentHashMap<Long, FileOutputStream> logOutputMap = new ConcurrentHashMap<>();

  private static File MIGRATING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "migrating")
              .toString());

  protected TsFileMigrationLogger() {
    if (MIGRATING_LOG_DIR == null) {
      logger.error("MIGRATING_LOG_DIR is null");
    }

    if (!MIGRATING_LOG_DIR.exists()) {
      if (MIGRATING_LOG_DIR.mkdirs()) {
        logger.info("MIGRATING_LOG_DIR {} created successfully", MIGRATING_LOG_DIR);
      } else {
        logger.error("MIGRATING_LOG_DIR {} create error", MIGRATING_LOG_DIR);
      }
      return;
    }

    if (!MIGRATING_LOG_DIR.isDirectory()) {
      logger.error("{} already exists but is not directory", MIGRATING_LOG_DIR);
    }
  }

  @Override
  public void close() {
    for (FileOutputStream logFileStream : logOutputMap.values()) {
      if (logFileStream != null) {
        try {
          logFileStream.close();
        } catch (IOException e) {
          logger.error("log file could not be closed");
        }
      }
    }
  }

  // singleton
  private static class MigratingFileLogManagerHolder {
    private MigratingFileLogManagerHolder() {}

    private static final TsFileMigrationLogger INSTANCE = new TsFileMigrationLogger();
  }

  public static TsFileMigrationLogger getInstance() {
    return MigratingFileLogManagerHolder.INSTANCE;
  }

  /**
   * started the migration task, write to
   *
   * @return true if write log successful, false otherwise
   */
  public boolean startTask(long taskId, File targetDir) throws IOException {
    FileOutputStream logFileOutput;
    if (logOutputMap.containsKey(taskId) && logOutputMap.get(taskId) != null) {
      logOutputMap.get(taskId).close();
      logOutputMap.remove(taskId);
    }
    File logFile = SystemFileFactory.INSTANCE.getFile(MIGRATING_LOG_DIR, taskId + ".log");
    if (logFile.exists()) {
      // want an empty log file
      logFile.delete();
    }
    if (!logFile.createNewFile()) {
      // log file doesn't exist but cannot be created
      return false;
    }

    logFileOutput = new FileOutputStream(logFile);
    logOutputMap.put(taskId, logFileOutput);

    ReadWriteIOUtils.write(targetDir.getAbsolutePath(), logFileOutput);
    logFileOutput.flush();

    return true;
  }

  /**
   * started migrating tsfile and its resource/mod files
   *
   * @return true if write log successful, false otherwise
   */
  public boolean start(long taskId, File tsfile) throws IOException {
    FileOutputStream logFileOutput;
    if (logOutputMap.containsKey(taskId) && logOutputMap.get(taskId) != null) {
      logFileOutput = logOutputMap.get(taskId);
    } else {
      File logFile = SystemFileFactory.INSTANCE.getFile(MIGRATING_LOG_DIR, taskId + ".log");
      if (!logFile.exists()) {
        if (!logFile.createNewFile()) {
          // log file doesn't exist but cannot be created
          return false;
        }
      }

      logFileOutput = new FileOutputStream(logFile);
      logOutputMap.put(taskId, logFileOutput);
    }

    ReadWriteIOUtils.write(tsfile.getAbsolutePath(), logFileOutput);
    logFileOutput.flush();

    return true;
  }

  /** finished migrating task, deletes logs and closes FileOutputStream */
  public void finish(long taskId) {
    File logFile = SystemFileFactory.INSTANCE.getFile(MIGRATING_LOG_DIR, taskId + ".log");
    if (logFile.exists()) {
      logFile.delete();
    }
    if (logOutputMap.containsKey(taskId)) {
      try {
        logOutputMap.get(taskId).close();
      } catch (IOException e) {
        logger.error("could not close fileoutputstream for task {}", taskId);
      }
      logOutputMap.remove(taskId);
    }
  }

  /** finish the unfinished MigrationTasks using log files under MIGRATING_LOG_DIR */
  public void recover() {
    for (File logFile : MIGRATING_LOG_DIR.listFiles()) {
      FileInputStream logFileInput;
      File targetDir;
      String tsfilePath;
      File tsfile;

      try {
        logFileInput = new FileInputStream(logFile);
        String targetDirPath = ReadWriteIOUtils.readString(logFileInput);

        targetDir = SystemFileFactory.INSTANCE.getFile(targetDirPath);
        tsfilePath = ReadWriteIOUtils.readString(logFileInput);

        if (targetDir.exists()) {
          if (!targetDir.isDirectory()) {
            logger.error("target dir {} not a directory", targetDirPath);
            continue;
          }
        } else if (!targetDir.mkdirs()) {
          logger.error("create target dir {} failed", targetDirPath);
          continue;
        }
      } catch (IOException e) {
        // could not read log file, continue to next log
        logger.error("MigratingFileLogManager: log file not found");
        continue;
      }

      while (tsfilePath != null && !tsfilePath.isEmpty()) {
        tsfile = SystemFileFactory.INSTANCE.getFile(tsfilePath);

        TsFileResource resource = new TsFileResource(tsfile);
        resource.migrate(targetDir);

        try {
          tsfilePath = ReadWriteIOUtils.readString(logFileInput);
        } catch (IOException e) {
          // finished reading all tsfile paths
          break;
        }
      }
      String filename = logFile.getName();
      long taskId = Long.parseLong(filename.substring(0, filename.lastIndexOf('.')));
      finish(taskId);
    }
  }
}