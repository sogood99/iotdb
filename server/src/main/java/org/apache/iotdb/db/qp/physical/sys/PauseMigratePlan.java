package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.List;

public class PauseMigratePlan extends PhysicalPlan {
  private long idx;
  private PartialPath storageGroup;
  private boolean pause = true;

  public PauseMigratePlan(boolean pause) {
    super(Operator.OperatorType.PAUSE_MIGRATE);
    this.pause = pause;
  }

  public PauseMigratePlan(PartialPath storageGroup, boolean pause) {
    super(Operator.OperatorType.PAUSE_MIGRATE);
    this.storageGroup = storageGroup;
    this.pause = pause;
  }

  public PauseMigratePlan(long idx, boolean pause) {
    super(Operator.OperatorType.PAUSE_MIGRATE);
    this.idx = idx;
    this.pause = pause;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public long getIndex() {
    return idx;
  }

  public boolean isPause() {
    return pause;
  }

  public PartialPath getStorageGroup() {
    return storageGroup;
  }
}
