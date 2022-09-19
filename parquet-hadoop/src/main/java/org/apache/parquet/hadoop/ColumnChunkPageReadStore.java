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
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;

import java.util.concurrent.LinkedBlockingDeque;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: should this actually be called RowGroupImpl or something?
 * The name is kind of confusing since it references three different "entities"
 * in our format: columns, chunks, and pages
 *
 */
public class ColumnChunkPageReadStore implements PageReadStore, DictionaryPageReadStore {
  private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReadStore.class);

  /**
   * PageReader for a single column chunk. A column chunk contains
   * several pages, which are yielded one by one in order.
   *
   * This implementation is provided with a list of pages, each of which
   * is decompressed and passed through.
   */
  public static final class ColumnChunkPageReader implements PageReader {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkPageReader.class);

    private final BytesInputDecompressor decompressor;
    private final long valueCount;
    private final LinkedBlockingDeque<Optional<DataPage>> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    // null means no page synchronization is required; firstRowIndex will not be returned by the pages
    private final OffsetIndex offsetIndex;
    private final long rowCount;
    private final boolean useOffHeapBuffer;
    private final boolean returnCompressedPages;
    private int pageIndex = 0;

    private final BlockCipher.Decryptor blockDecryptor;
    private final byte[] dataPageAAD;
    private final byte[] dictionaryPageAAD;

    private final ParquetFileReader.PageReader pageReader;

    /**
     * Whether we've finished reading all the pages in this reader store.
     */
    private boolean isFinished;

    ColumnChunkPageReader(BytesInputDecompressor decompressor,
        LinkedBlockingDeque<Optional<DataPage>> compressedPages,
        DictionaryPage compressedDictionaryPage, OffsetIndex offsetIndex, long valueCount,
        long rowCount, BlockCipher.Decryptor blockDecryptor, byte[] fileAAD, int rowGroupOrdinal,
        int columnOrdinal, ParquetFileReader.PageReader pageReader, ParquetReadOptions options) {
      this.decompressor = decompressor;
      this.compressedPages = compressedPages;
      this.compressedDictionaryPage = compressedDictionaryPage;
      this.valueCount = valueCount;
      this.offsetIndex = offsetIndex;
      this.rowCount = rowCount;
      this.useOffHeapBuffer = options.useOffHeapBuffer();
      this.returnCompressedPages = options.isCompressedPagesEnabled();

      this.blockDecryptor = blockDecryptor;
      this.pageReader = pageReader;
      this.isFinished = false;

      if (null != blockDecryptor) {
        dataPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DataPage, rowGroupOrdinal, columnOrdinal, 0);
        dictionaryPageAAD = AesCipher.createModuleAAD(fileAAD, ModuleType.DictionaryPage, rowGroupOrdinal, columnOrdinal, -1);
      } else {
        dataPageAAD = null;
        dictionaryPageAAD = null;
      }
    }

    @Override
    public void close() throws IOException {
      this.pageReader.close();
    }

    private int getPageOrdinal(int currentPageIndex) {
      if (null == offsetIndex) {
        return currentPageIndex;
      }

      return offsetIndex.getPageOrdinal(currentPageIndex);
    }

    @Override
    public long getTotalValueCount() {
      return valueCount;
    }

    public int getPageValueCount() {
      Preconditions.checkState(!isFinished,
        "getPageValueCount shouldn't be called after all pages have been read");
      final Optional<DataPage> compressedPage;
      try {
        // Since there is no blocking peek, take the head and added it back
        compressedPage = compressedPages.take();
        Preconditions.checkState(compressedPage.isPresent(), "Page should be non-empty!");
      } catch (InterruptedException e) {
        throw new RuntimeException("Error reading parquet page data.") ;
      }
      compressedPages.addFirst(compressedPage);
      return compressedPage.get().getValueCount();
    }

    public void skipPage() {
      Optional<DataPage> page = compressedPages.remove();
      Preconditions.checkState(page.isPresent(), "Page to be skipped should be non-empty!");
      pageIndex++;
    }

    @Override
    public DataPage readPage() {
      if (isFinished) {
        // All the pages have been read. Simply return null in case this is called again.
        return null;
      }
      final DataPage compressedPage;
      try {
        compressedPage = compressedPages.take().orElse(null);
      } catch (InterruptedException e) {
        throw new RuntimeException("Error reading parquet page data.") ;
      }
      if (compressedPage == null) {
        isFinished = true;
        return null;
      }
      final int currentPageIndex = pageIndex++;

      if (null != blockDecryptor) {
        AesCipher.quickUpdatePageAAD(dataPageAAD, getPageOrdinal(currentPageIndex));
      }

      return compressedPage.accept(new DataPage.Visitor<DataPage>() {
        @Override
        public DataPage visit(DataPageV1 dataPageV1) {
          Preconditions.checkState(dataPageV1.isCompressed(),
            "Expect input V1 data page to be compressed!");

          try {
            BytesInput bytes = dataPageV1.getBytes();
            BytesInput pageBytes;

            if (useOffHeapBuffer) {
              // This is the buffer after decryption, but before decompression
              ByteBuffer byteBuffer;
              long compressedSize = bytes.size();

              if (blockDecryptor == null) {
                byteBuffer = bytes.toByteBuffer();
              } else {
                byte[] decrypted = blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD);
                compressedSize = decrypted.length;
                // TODO: we should allocate this from `ByteBufferAllocator`
                byteBuffer = ByteBuffer.allocateDirect((int) compressedSize);
                byteBuffer.put(decrypted);
                byteBuffer.flip();
              }

              // Close the original bytes input from the page now
              bytes.close();

              if (!byteBuffer.isDirect()) {
                ByteBuffer directByteBuffer = ByteBuffer.allocateDirect((int) compressedSize);
                directByteBuffer.put(byteBuffer);
                directByteBuffer.flip();
                if (byteBuffer instanceof AutoCloseable) {
                  ((AutoCloseable) byteBuffer).close();
                }
                byteBuffer = directByteBuffer;
              }

              if (returnCompressedPages) {
                pageBytes = BytesInput.from(byteBuffer);
              } else {
                // The input/output bytebuffers must be direct for (bytebuffer-based, native)
                // decompressor
                ByteBuffer decompressedBuffer =
                  ByteBuffer.allocateDirect(dataPageV1.getUncompressedSize());
                decompressor.decompress(byteBuffer, (int) compressedSize, decompressedBuffer,
                  dataPageV1.getUncompressedSize());

                // HACKY: sometimes we need to do `flip` because the position of output bytebuffer is
                // not reset.
                if (decompressedBuffer.position() != 0) {
                  decompressedBuffer.flip();
                }
                pageBytes = BytesInput.from(decompressedBuffer);
                if (byteBuffer instanceof AutoCloseable) {
                  ((AutoCloseable) byteBuffer).close();
                }
              }
            } else { // use on-heap buffer
              if (null != blockDecryptor) {
                BytesInput oldBytes = bytes;
                bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dataPageAAD));
                oldBytes.close();
              }
              if (returnCompressedPages) {
                pageBytes = bytes;
              } else {
                pageBytes = decompressor.decompress(bytes, dataPageV1.getUncompressedSize());
                bytes.close();
              }
            }

            final DataPageV1 decompressedPage;
            if (offsetIndex == null) {
              decompressedPage = new DataPageV1(
                  pageBytes,
                  dataPageV1.getValueCount(),
                  dataPageV1.getUncompressedSize(),
                  dataPageV1.getStatistics(),
                  dataPageV1.getRlEncoding(),
                  dataPageV1.getDlEncoding(),
                  dataPageV1.getValueEncoding(),
                  returnCompressedPages);
            } else {
              long firstRowIndex = offsetIndex.getFirstRowIndex(currentPageIndex);
              decompressedPage = new DataPageV1(
                  pageBytes,
                  dataPageV1.getValueCount(),
                  dataPageV1.getUncompressedSize(),
                  firstRowIndex,
                  Math.toIntExact(offsetIndex.getLastRowIndex(currentPageIndex, rowCount) - firstRowIndex + 1),
                  dataPageV1.getStatistics(),
                  dataPageV1.getRlEncoding(),
                  dataPageV1.getDlEncoding(),
                  dataPageV1.getValueEncoding(),
                  returnCompressedPages);
            }
            if (dataPageV1.getCrc().isPresent()) {
              decompressedPage.setCrc(dataPageV1.getCrc().getAsInt());
            }
            return decompressedPage;
          } catch (Exception e) {
            throw new ParquetDecodingException("could not decompress page", e);
          }
        }

        @Override
        public DataPage visit(DataPageV2 dataPageV2) {
          if (!returnCompressedPages && !dataPageV2.isCompressed() &&  offsetIndex == null &&
              null == blockDecryptor) {
            return dataPageV2;
          }
          BytesInput pageBytes = dataPageV2.getData();

          if (null != blockDecryptor) {
            try {
              BytesInput oldPageBytes = pageBytes;
              pageBytes = BytesInput.from(blockDecryptor.decrypt(pageBytes.toByteArray(), dataPageAAD));
              oldPageBytes.close();
            } catch (Exception e) {
              throw new ParquetDecodingException("could not convert page ByteInput to byte array", e);
            }
          }
          if (!returnCompressedPages && dataPageV2.isCompressed()) {
            int uncompressedSize = Math.toIntExact(
                dataPageV2.getUncompressedSize()
                    - dataPageV2.getDefinitionLevels().size()
                    - dataPageV2.getRepetitionLevels().size());
            try {
              BytesInput oldPageBytes = pageBytes;
              pageBytes = decompressor.decompress(pageBytes, uncompressedSize);
              oldPageBytes.close();
            } catch (Exception e) {
              throw new ParquetDecodingException("could not decompress page", e);
            }
          }

          if (offsetIndex == null) {
            return new DataPageV2(
                dataPageV2.getRowCount(),
                dataPageV2.getNullCount(),
                dataPageV2.getValueCount(),
                dataPageV2.getRepetitionLevels(),
                dataPageV2.getDefinitionLevels(),
                dataPageV2.getDataEncoding(),
                pageBytes,
                dataPageV2.getUncompressedSize(),
                dataPageV2.getStatistics(),
                returnCompressedPages);
          } else {
            return new DataPageV2(
                dataPageV2.getRowCount(),
                dataPageV2.getNullCount(),
                dataPageV2.getValueCount(),
                offsetIndex.getFirstRowIndex(currentPageIndex),
                dataPageV2.getRepetitionLevels(),
                dataPageV2.getDefinitionLevels(),
                dataPageV2.getDataEncoding(),
                pageBytes,
                dataPageV2.getUncompressedSize(),
                dataPageV2.getStatistics(),
                returnCompressedPages);
          }
        }
      });
    }

    @Override
    public DictionaryPage readDictionaryPage() {
      if (compressedDictionaryPage == null) {
        return null;
      }
      try {
        BytesInput bytes = compressedDictionaryPage.getBytes();
        if (null != blockDecryptor) {
          bytes = BytesInput.from(blockDecryptor.decrypt(bytes.toByteArray(), dictionaryPageAAD));
        }
        if (!returnCompressedPages) {
          bytes = decompressor.decompress(bytes, compressedDictionaryPage.getUncompressedSize());
        }
        DictionaryPage result = new DictionaryPage(
          bytes,
          compressedDictionaryPage.getDictionarySize(),
          compressedDictionaryPage.getEncoding(),
          returnCompressedPages);
        if (compressedDictionaryPage.getCrc().isPresent()) {
          result.setCrc(compressedDictionaryPage.getCrc().getAsInt());
        }
        return result;
      } catch (IOException e) {
        throw new ParquetDecodingException("Could not decompress dictionary page", e);
      }
    }
  }

  private final Map<ColumnDescriptor, ColumnChunkPageReader> readers = new HashMap<ColumnDescriptor, ColumnChunkPageReader>();
  private final long rowCount;
  private final RowRanges rowRanges;

  public ColumnChunkPageReadStore(long rowCount) {
    this.rowCount = rowCount;
    rowRanges = null;
  }

  ColumnChunkPageReadStore(RowRanges rowRanges) {
    this.rowRanges = rowRanges;
    rowCount = rowRanges.rowCount();
  }

  // Close `ParquetFileReader.PageReader`
  public void close() throws IOException {
    for (PageReader pageReader : readers.values()) {
      pageReader.close();
    }
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public PageReader getPageReader(ColumnDescriptor path) {
    final PageReader pageReader = readers.get(path);
    if (pageReader == null) {
      throw new IllegalArgumentException(path + " is not in the store: " + readers.keySet() + " " + rowCount);
    }
    return pageReader;
  }

  @Override
  public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
    return readers.get(descriptor).readDictionaryPage();
  }

  @Override
  public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
    return rowRanges == null ? Optional.empty() : Optional.of(rowRanges.iterator());
  }

  void addColumn(ColumnDescriptor path, ColumnChunkPageReader reader) {
    if (readers.put(path, reader) != null) {
      throw new RuntimeException(path+ " was added twice");
    }
  }
}
