package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.PauseMigratePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class PauseMigrateOperator extends Operator {

  // MigrateTask index
  private long idx = -1;
  private PartialPath storageGroup;

  public PauseMigrateOperator(int tokenIntType) {
    super(tokenIntType);
    this.operatorType = OperatorType.PAUSE_MIGRATE;
  }

  public long getIndex() {
    return idx;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }

  public void setIndex(long idx) {
    this.idx = idx;
  }

  public void setStorageGroup(PartialPath storageGroup) {
    this.storageGroup = storageGroup;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    if (storageGroup != null) {
      return new PauseMigratePlan(storageGroup, true);
    } else if (idx != -1) {
      return new PauseMigratePlan(idx, true);
    } else {
      return new PauseMigratePlan(true);
    }
  }
}
