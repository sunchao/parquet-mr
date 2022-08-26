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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;
import org.xerial.snappy.Snappy;

import org.apache.parquet.Preconditions;

public class SnappyDecompressor extends DirectBufferDecompressorBase implements Decompressor {

  @Override
  protected int getUncompressedLength(ByteBuffer src) throws IOException {
    return Snappy.uncompressedLength(src);
  }

  @Override
  protected int decompressDirect(ByteBuffer src, ByteBuffer dest) throws IOException {
    return Snappy.uncompress(src, dest);
  }


} //class SnappyDecompressor
