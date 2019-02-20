package org.apache.beam.sdk.extensions.sql;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.TimeNanosecondType;
import org.junit.Test;

public class BeamSqlDslLogicalTypesTest {
  @Test
  public void testTimeNanosecondType() {
    LocalTime localTime = LocalTime.of(13, 44, 59, 999999999);
    TimeNanosecondType timeNanosecondType = new TimeNanosecondType();
    assertEquals(localTime, timeNanosecondType.toInputType(timeNanosecondType.toBaseType(localTime)));
  }
}
