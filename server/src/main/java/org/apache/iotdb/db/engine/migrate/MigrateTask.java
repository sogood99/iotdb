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
package org.apache.iotdb.db.engine.migrate;

import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.File;

public class MigrateTask {
  private long taskId = -1;
  private PartialPath storageGroup = null;
  private File targetDir = null;
  private long startTime = Long.MAX_VALUE;
  private long ttl = Long.MAX_VALUE;
  private MigrateTaskStatus status = MigrateTaskStatus.READY;

  public MigrateTask(
      long taskId, PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    this.taskId = taskId;
    this.storageGroup = storageGroup;
    this.targetDir = targetDir;
    this.ttl = ttl;
    this.startTime = startTime;
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

  public MigrateTaskStatus getStatus() {
    return status;
  }

  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  public void setStorageGroup(PartialPath storageGroup) {
    this.storageGroup = storageGroup;
  }

  public void setTargetDir(File targetDir) {
    this.targetDir = targetDir;
  }

  public void setTTL(long ttl) {
    this.ttl = ttl;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setStatus(MigrateTaskStatus status) {
    this.status = status;
  }

  public enum MigrateTaskStatus {
    READY,
    RUNNING,
    UNSET,
    PAUSED,
    CANCELING,
    ERROR,
    FINISHED
  }
}
