package org.apache.beam.sdk.extensions.sql.meta.provider.hcatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.catalog.ReadableCatalog;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

@Experimental
public class HiveCatalog extends ReadableCatalog {
  private List<String> dbList;

  private HiveConf hiveConf;

  private Map<String, Table> tableMap;

  public HiveCatalog(HiveConf hiveConf, List<String> dbList) throws MetaException {
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

  @Override
  public void fetchExternalMetadata() throws MetaException {
    tableMap = new HashMap<>();
    if (dbList.size() == 0) {
      return;
    }

    HiveMetaStoreClient storeClient = new HiveMetaStoreClient(hiveConf);
    for (String dbName : dbList) {
      List<String> tableNames = storeClient.getAllTables(dbName);
    }
  }
}
