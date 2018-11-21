package org.apache.beam.sdk.extensions.sql.meta.catalog;

import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

@Experimental
abstract class ReadableCatalog implements TableProvider {

  @Override
  public final void createTable(Table table) {
    // No-op
  }

  @Override
  public final void dropTable(String tableName) {
    // No-op
  }
}
