package org.apache.beam.sdk.extensions.sql.meta.provider.hcatalog;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

@Experimental
public class HCatalogTable extends BaseBeamTable {

  public HCatalogTable(Schema schema) {
    super(schema);
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return null;
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return null;
  }
}
