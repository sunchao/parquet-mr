package org.apache.parquet.hadoop.util;

import org.junit.Test;

import static org.apache.parquet.hadoop.util.HadoopInputFile.isHadoop3;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestHadoopInputFile {
  @Test
  public void testIsHadoop3() {
    assertTrue(isHadoop3("3.3.0"));
    assertTrue(isHadoop3("3.4.0-SNAPSHOT"));
    assertTrue(isHadoop3("3.1.2-apple"));
    assertTrue(isHadoop3("3.0.2.5-apple"));
    assertTrue(isHadoop3("3.2"));

    assertFalse(isHadoop3("2.7.2"));
    assertFalse(isHadoop3("2.7.3-SNAPSHOT"));
    assertFalse(isHadoop3("2.7"));
    assertFalse(isHadoop3("2"));
    assertFalse(isHadoop3("3"));
    assertFalse(isHadoop3("3-SNAPSHOT"));
    assertFalse(isHadoop3("3.2-SNAPSHOT"));
  }
}
