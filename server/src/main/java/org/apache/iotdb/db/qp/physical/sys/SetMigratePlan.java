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

package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class SetMigratePlan extends PhysicalPlan {

  private PartialPath storageGroup;
  private File targetDir;
  private long dataTTL;
  private long startTime;

  public SetMigratePlan() {
    super(OperatorType.MIGRATE);
  }

  public SetMigratePlan(PartialPath storageGroup, File targetDir, long dataTTL, long startTime) {
    // set migrate
    super(OperatorType.MIGRATE);
    this.storageGroup = storageGroup;
    this.targetDir = targetDir;
    this.dataTTL = dataTTL;
    this.startTime = startTime;
  }

  public SetMigratePlan(PartialPath storageGroup) {
    // unset migrate
    this(storageGroup, null, Long.MAX_VALUE, Long.MAX_VALUE);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.MIGRATE.ordinal();
    stream.writeByte((byte) type);
    stream.writeLong(dataTTL);
    stream.writeLong(startTime);
    putString(stream, storageGroup.getFullPath());
    putString(stream, targetDir.getAbsolutePath());

    stream.writeLong(index);
  }

  @Override
  public void serializeImpl(ByteBuffer buffer) {
    int type = PhysicalPlanType.TTL.ordinal();
    buffer.put((byte) type);
    buffer.putLong(dataTTL);
    buffer.putLong(startTime);
    putString(buffer, storageGroup.getFullPath());
    putString(buffer, targetDir.getAbsolutePath());

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.dataTTL = buffer.getLong();
    this.startTime = buffer.getLong();
    this.storageGroup = new PartialPath(readString(buffer));
    this.targetDir = new File(readString(buffer));

    this.index = buffer.getLong();
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(PartialPath storageGroup) {
    this.storageGroup = storageGroup;
  }

  public File getTargetDir() {
    return targetDir;
  }

  public void setTargetDir(File targetDir) {
    this.targetDir = targetDir;
  }

  public long getDataTTL() {
    return dataTTL;
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
