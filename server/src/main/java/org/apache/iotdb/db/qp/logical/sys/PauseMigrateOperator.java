package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.PauseMigratePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class PauseMigrateOperator extends Operator {

  private long taskId = -1;
  private PartialPath storageGroup;

  public PauseMigrateOperator(int tokenIntType) {
    super(tokenIntType);
    this.operatorType = OperatorType.PAUSE_MIGRATE;
  }

  public long getTaskId() {
    return taskId;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  public void setStorageGroup(PartialPath storageGroup) {
    this.storageGroup = storageGroup;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    if (storageGroup != null) {
      return new PauseMigratePlan(storageGroup, true);
    } else if (taskId != -1) {
      return new PauseMigratePlan(taskId, true);
    } else {
      return new PauseMigratePlan(true);
    }
  }
}
