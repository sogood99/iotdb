/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;

/** Class for each Migration Task */
public class MigrationTask {
  private long taskId;
  private PartialPath storageGroup;
  private File targetDir;
  private long startTime;
  private long ttl;
  private volatile MigrationTaskStatus status = MigrationTaskStatus.READY;
  private long submitTime;

  private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  FileOutputStream logFileOutput = null;
  private static File MIGRATING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "migrating")
              .toString());

  public MigrationTask(long taskId) {
    this.taskId = taskId;
  }

  public MigrationTask(MigrationTask task) {
    this.taskId = task.getTaskId();
    this.storageGroup = task.getStorageGroup();
    this.targetDir = task.getTargetDir();
    this.ttl = task.getTTL();
    this.startTime = task.getStartTime();
    this.submitTime = task.getSubmitTime();
  }

  public MigrationTask(
      long taskId, PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    this.taskId = taskId;
    this.storageGroup = storageGroup;
    this.targetDir = targetDir;
    this.ttl = ttl;
    this.startTime = startTime;
    this.submitTime = DatetimeUtils.currentTime();
  }

  public MigrationTask(
      long taskId,
      PartialPath storageGroup,
      File targetDir,
      long ttl,
      long startTime,
      long submitTime) {
    this.taskId = taskId;
    this.storageGroup = storageGroup;
    this.targetDir = targetDir;
    this.ttl = ttl;
    this.startTime = startTime;
    this.submitTime = submitTime;
  }

  /**
   * started the migration task, write to
   *
   * @return true if write log successful, false otherwise
   */
  public boolean startTask() throws IOException {
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

    ReadWriteIOUtils.write(targetDir.getAbsolutePath(), logFileOutput);
    logFileOutput.flush();

    return true;
  }

  /**
   * started migrating tsfile and its resource/mod files
   *
   * @return true if write log successful, false otherwise
   */
  public boolean startFile(File tsfile) throws IOException {
    if (logFileOutput == null) {
      logger.error("need to run MigrationTask.startTask before MigrationTask.start");
      return false;
    }

    ReadWriteIOUtils.write(tsfile.getAbsolutePath(), logFileOutput);
    logFileOutput.flush();

    return true;
  }

  /** finished migrating task, deletes logs and closes FileOutputStream */
  public void finish() {
    File logFile = SystemFileFactory.INSTANCE.getFile(MIGRATING_LOG_DIR, taskId + ".log");
    if (logFile.exists()) {
      logFile.delete();
    }
    this.close();
  }

  /** release all resources */
  public void close() {
    try {
      if (logFileOutput != null) {
        logFileOutput.close();
        logFileOutput = null;
      }
    } catch (IOException e) {
      logger.error("could not close fileoutputstream for task {}", taskId);
    }
  }

  // getter and setter functions

  public long getTaskId() {
    return taskId;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public File getTargetDir() {
    return targetDir;
  }

  public long getTTL() {
    return ttl;
  }

  public long getStartTime() {
    return startTime;
  }

  public MigrationTaskStatus getStatus() {
    return status;
  }

  public long getSubmitTime() {
    return submitTime;
  }

  public void setStatus(MigrationTaskStatus status) {
    this.status = status;
  }

  public enum MigrationTaskStatus {
    READY,
    RUNNING,
    PAUSED,
    CANCELED,
    ERROR,
    FINISHED
  }
}
