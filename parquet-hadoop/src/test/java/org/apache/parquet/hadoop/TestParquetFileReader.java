package org.apache.parquet.hadoop;

import java.io.File;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.Page;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestParquetFileReader {
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  @SuppressWarnings("deprecation")
  public void testCompressedPages() throws Exception {
    File testFile = temp.newFile();
    testFile.delete(); // delete so we can create a new file later

    Configuration conf = new Configuration();
    MessageType schema = MessageTypeParser.parseMessageType(
      "message root { " +
        "required binary _1(UTF8); " +
        "}"
    );
    ColumnDescriptor descriptor = schema.getColumns().get(0);
    Path path = new Path(testFile.toURI());

    Statistics<?> stats = Statistics.getBuilderForReading(
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY,
          Type.Repetition.REQUIRED).named("_1"))
      .build();
    BytesInput bytes = BytesInput.from(new byte[]{ 0, 1, 2, 3 });
    ParquetFileWriter writer = new ParquetFileWriter(conf, schema, path);
    writer.start();
    writer.startBlock(3);
    writer.startColumn(descriptor, 5, CompressionCodecName.GZIP);
    writer.writeDictionaryPage(new DictionaryPage(bytes, 5, Encoding.RLE_DICTIONARY, true));
    writer.writeDataPage(5, 5, bytes, stats, 5, Encoding.RLE, Encoding.RLE, Encoding.PLAIN);
    writer.writeDataPageV2(5, 0, 5, bytes, bytes, Encoding.DELTA_BYTE_ARRAY, bytes, 4, stats);
    writer.endColumn();
    writer.endBlock();
    writer.end(new HashMap<>());

    for (boolean isCompressed : new boolean[]{ true, false }) {
      ParquetReadOptions options = ParquetReadOptions.builder()
        .enableCompressedPages(isCompressed)
        .build();
      ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(path, conf), options);
      PageReadStore rowGroup = reader.readNextRowGroup();
      PageReader column = rowGroup.getPageReader(descriptor);

      DictionaryPage dictionaryPage = column.readDictionaryPage();
      assertNotNull(dictionaryPage);
      assertEquals(dictionaryPage.isCompressed(), isCompressed);
      while (true) {
        Page page = column.readPage();
        if (page == null) break;
        assertEquals(page.isCompressed(), isCompressed);
      }
    }
  }
}
