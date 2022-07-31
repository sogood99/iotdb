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
 *
 */

package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.SetMigratePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class UnsetMigrateOperator extends Operator {

  private PartialPath storageGroup = null;
  private long idx = -1;

  public UnsetMigrateOperator(int tokenIntType) {
    super(tokenIntType);
    this.operatorType = OperatorType.MIGRATE;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public void setStorageGroup(PartialPath storageGroup) {
    this.storageGroup = storageGroup;
  }

  public long getIndex() {
    return idx;
  }

  public void setIndex(long idx) {
    this.idx = idx;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    if (storageGroup != null) {
      return new SetMigratePlan(storageGroup);
    } else if (idx != -1) {
      return new SetMigratePlan(idx);
    } else {
      return new SetMigratePlan();
    }
  }
}
