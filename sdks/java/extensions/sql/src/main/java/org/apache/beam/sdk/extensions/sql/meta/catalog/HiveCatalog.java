package org.apache.beam.sdk.extensions.sql.meta.catalog;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.hadoop.hive.conf.HiveConf;

@Experimental
public class HiveCatalog extends ReadableCatalog {
  private List<String> dbList;

  private HiveConf hiveConf;

  public HiveCatalog(HiveConf hiveConf, List<String> dbList) {
    this.hiveConf = hiveConf;
    this.dbList = dbList;
  }

  @Override
  public String getTableType() {
    return "hcatalog";
  }

  @Override
  public Map<String, Table> getTables() {
    return null;
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return null;
  }
}
