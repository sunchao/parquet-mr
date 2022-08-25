/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.page;

import java.io.IOException;
import java.util.Objects;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;

/**
 * Data for a dictionary page
 */
public class DictionaryPage extends Page {

  private final BytesInput bytes;
  private final int dictionarySize;
  private final Encoding encoding;

  /**
   * Creates a compressed dictionary page
   *
   * @param bytes the content of the page
   * @param uncompressedSize the size uncompressed
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public static DictionaryPage compressed(BytesInput bytes, int uncompressedSize,
      int dictionarySize, Encoding encoding) {
    return new DictionaryPage(bytes, uncompressedSize, dictionarySize, encoding, true);
  }

  /**
   * Creates an uncompressed dictionary page
   *
   * @param bytes the content of the page
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public static DictionaryPage uncompressed(BytesInput bytes, int dictionarySize,
      Encoding encoding) {
    return new DictionaryPage(bytes, dictionarySize, encoding, false);
  }

  /**
   * Creates an uncompressed dictionary page
   *
   * @param bytes the content of the page
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public DictionaryPage(BytesInput bytes, int dictionarySize, Encoding encoding) {
    this(bytes, (int)bytes.size(), dictionarySize, encoding, false); // TODO: fix sizes long or int
  }

  /**
   * Creates a dictionary page
   *
   * @param bytes the content of the page
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   * @param isCompressed whether the page is compressed or not
   */
  public DictionaryPage(BytesInput bytes, int dictionarySize, Encoding encoding,
      boolean isCompressed) {
    this(bytes, (int)bytes.size(), dictionarySize, encoding, isCompressed); // TODO: fix sizes long or int
  }

  /**
   * Creates an uncompressed dictionary page
   *
   * @param bytes the (possibly compressed) content of the page
   * @param uncompressedSize the size uncompressed
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public DictionaryPage(BytesInput bytes, int uncompressedSize, int dictionarySize, Encoding encoding) {
    this(bytes, uncompressedSize, dictionarySize, encoding, false);
  }

  /**
   * Creates a dictionary page
   *
   * @param bytes the (possibly compressed) content of the page
   * @param uncompressedSize the size uncompressed
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   * @param isCompressed whether the page is compressed or not
   */
  public DictionaryPage(BytesInput bytes, int uncompressedSize, int dictionarySize,
      Encoding encoding, boolean isCompressed) {
    super(Math.toIntExact(bytes.size()), uncompressedSize, isCompressed);
    this.bytes = Objects.requireNonNull(bytes, "bytes cannot be null");
    this.dictionarySize = dictionarySize;
    this.encoding = Objects.requireNonNull(encoding, "encoding cannot be null");
  }

  public BytesInput getBytes() {
    return bytes;
  }

  public int getDictionarySize() {
    return dictionarySize;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public DictionaryPage copy() throws IOException {
    return new DictionaryPage(BytesInput.copy(bytes), getUncompressedSize(), dictionarySize,
      encoding, isCompressed);
  }


  @Override
  public String toString() {
    return "Page [bytes.size=" + bytes.size() + ", entryCount=" + dictionarySize + ", uncompressedSize=" + getUncompressedSize() + ", encoding=" + encoding + "]";
  }

}
