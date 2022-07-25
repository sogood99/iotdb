package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.util.List;

public class PauseMigratePlan extends PhysicalPlan {
  private List<PartialPath> storageGroups;

  public PauseMigratePlan(List<PartialPath> storageGroups) {
    super(Operator.OperatorType.PAUSE_MIGRATE);
    this.storageGroups = storageGroups;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public List<PartialPath> getStorageGroups() {
    return storageGroups;
  }
}
