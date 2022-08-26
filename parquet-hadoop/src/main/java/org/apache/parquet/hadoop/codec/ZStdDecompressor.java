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
package org.apache.parquet.hadoop.codec;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.apache.parquet.Preconditions;

public class ZStdDecompressor extends DirectBufferDecompressorBase implements Decompressor {

  @Override
  protected int getUncompressedLength(ByteBuffer src) throws IOException {
    return -1;
  }

  @Override
  protected int decompressDirect(ByteBuffer src, ByteBuffer dest) throws IOException {
    int pos = dest.position();
    int size = Zstd.decompress(dest, src);
    dest.position(pos);
    if (Zstd.isError(size)) {
      long errCode = Zstd.getErrorCode(size);
      String errMsg = Zstd.getErrorName(size);
      throw new IOException("Error decompressing zstd data. [" + errCode + "]: " + errMsg + ".");
    }
    return size;
  }

  public static class ZStdDirectDecompressor extends ZStdDecompressor implements
    DirectDecompressor {

    @Override
    public void decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
      if (!dst.isDirect()) {
        throw new UnsupportedOperationException(
          "Output buffer is not a direct ByteBuffer. ZStdDirectDecompressor only works with direct ByteBuffers");
      }
      if (!src.isDirect()) {
        throw new UnsupportedOperationException(
          "Input buffer is not a direct ByteBuffer. ZStdDirectDecompressor only works with direct ByteBuffers");
      }
      if (dst.remaining() <= 0) {
        throw new IOException("Insufficient space in output buffer");
      }
      decompressDirect(src, dst);
    }

  }

} //class
