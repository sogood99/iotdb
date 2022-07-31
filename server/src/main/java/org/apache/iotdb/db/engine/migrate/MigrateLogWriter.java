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

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.logfile.MLogTxtWriter;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class MigrateLogWriter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MLogTxtWriter.class);
  private final File logFile;
  private FileOutputStream logFileOutStream;

  public MigrateLogWriter(String logFileName) throws FileNotFoundException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
    if (!logFile.exists()) {
      if (logFile.getParentFile().mkdirs()) {
        logger.info("created migrate log folder");
      } else {
        logger.info("create migrate log folder failed");
      }
    }
    logFileOutStream = new FileOutputStream(logFile, true);
  }

  public MigrateLogWriter(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    logFileOutStream = new FileOutputStream(logFile, true);
  }

  private void putLog(MigrateLog log) {
    try {
      int type = log.type.ordinal();
      ReadWriteIOUtils.write((byte) type, logFileOutStream);
      ReadWriteIOUtils.write(log.index, logFileOutStream);

      if (log.type == LogType.SET) {
        ReadWriteIOUtils.write(log.storageGroup.getFullPath(), logFileOutStream);
        ReadWriteIOUtils.write(log.targetDirPath, logFileOutStream);
        ReadWriteIOUtils.write(log.startTime, logFileOutStream);
        ReadWriteIOUtils.write(log.ttl, logFileOutStream);
      }
    } catch (IOException e) {
      logger.error("unable to write to migrate log");
    }
  }

  public void setMigrate(MigrateTask migrateTask) throws IOException {
    MigrateLog log =
        new MigrateLog(
            LogType.SET,
            migrateTask.getIndex(),
            migrateTask.getStorageGroup(),
            migrateTask.getTargetDir().getPath(),
            migrateTask.getStartTime(),
            migrateTask.getTTL());
    putLog(log);
  }

  public void startMigrate(MigrateTask migrateTask) throws IOException {
    MigrateLog log = new MigrateLog(LogType.START, migrateTask.getIndex());
    putLog(log);
  }

  public void finishMigrate(MigrateTask migrateTask) throws IOException {
    MigrateLog log = new MigrateLog(LogType.FINISHED, migrateTask.getIndex());
    putLog(log);
  }

  public void unsetMigrate(MigrateTask migrateTask) throws IOException {
    MigrateLog log = new MigrateLog(LogType.UNSET, migrateTask.getIndex());
    putLog(log);
  }

  public void pauseMigrate(MigrateTask migrateTask) throws IOException {
    MigrateLog log = new MigrateLog(LogType.PAUSE, migrateTask.getIndex());
    putLog(log);
  }

  public void unpauseMigrate(MigrateTask migrateTask) throws IOException {
    MigrateLog log = new MigrateLog(LogType.UNPAUSE, migrateTask.getIndex());
    putLog(log);
  }

  public void error(MigrateTask migrateTask) throws IOException {
    MigrateLog log = new MigrateLog(LogType.ERROR, migrateTask.getIndex());
    putLog(log);
  }

  @Override
  public void close() throws Exception {
    logFileOutStream.close();
  }

  public static class MigrateLog {
    public LogType type;
    public long index;
    public PartialPath storageGroup;
    public String targetDirPath;
    public long startTime;
    public long ttl;

    public MigrateLog() {}

    public MigrateLog(LogType type, long index) {
      this.type = type;
      this.index = index;
    }

    public MigrateLog(
        LogType type,
        long index,
        PartialPath storageGroup,
        String targetDirPath,
        long startTime,
        long ttl) {
      this.type = type;
      this.index = index;
      this.storageGroup = storageGroup;
      this.targetDirPath = targetDirPath;
      this.startTime = startTime;
      this.ttl = ttl;
    }
  }

  public enum LogType {
    SET,
    UNSET,
    START,
    PAUSE,
    UNPAUSE,
    FINISHED,
    ERROR
  }
}
