package org.apache.parquet.hadoop.util;

import org.junit.Test;

import static org.apache.parquet.hadoop.util.HadoopInputFile.isAtLeastHadoop33;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHadoopInputFile {
  @Test
  public void testIsAtLeastHadoop33() {
    assertTrue(isAtLeastHadoop33("3.3.0"));
    assertTrue(isAtLeastHadoop33("3.4.0-SNAPSHOT"));
    assertTrue(isAtLeastHadoop33("3.12.5"));
    assertTrue(isAtLeastHadoop33("3.20.6.4-apple"));

    assertFalse(isAtLeastHadoop33("2.7.2"));
    assertFalse(isAtLeastHadoop33("2.7.3-SNAPSHOT"));
    assertFalse(isAtLeastHadoop33("2.7"));
    assertFalse(isAtLeastHadoop33("2"));
    assertFalse(isAtLeastHadoop33("3"));
    assertFalse(isAtLeastHadoop33("3.2"));
    assertFalse(isAtLeastHadoop33("3.0.2.5-apple"));
    assertFalse(isAtLeastHadoop33("3.1.2-apple"));
    assertFalse(isAtLeastHadoop33("3-SNAPSHOT"));
    assertFalse(isAtLeastHadoop33("3.2-SNAPSHOT"));
  }
}
