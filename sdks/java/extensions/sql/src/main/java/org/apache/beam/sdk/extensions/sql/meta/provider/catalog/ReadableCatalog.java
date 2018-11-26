package org.apache.beam.sdk.extensions.sql.meta.provider.catalog;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;

@Experimental
public abstract class ReadableCatalog implements TableProvider {

  @Override
  public final void createTable(Table table) {
    // No-op
  }

  @Override
  public final void dropTable(String tableName) {
    // No-op
  }

  @Override
  public Map<String, Table> getTables() {
    return Collections.emptyMap();
  }

  public abstract void fetchExternalMetadata() throws Exception;
}
