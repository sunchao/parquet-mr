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
package org.apache.parquet.bytes;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This ByteBufferInputStream does not consume the ByteBuffer being passed in,
 * but will create a slice of the current buffer.
 */
class SingleBufferInputStream extends ByteBufferInputStream {

  private final ParquetBuf buffer;
  private final long startPosition;
  private int mark = -1;

  SingleBufferInputStream(ParquetBuf buffer) {
    // duplicate the buffer because its state will be modified
    this.buffer = buffer.duplicate();
    this.startPosition = buffer.readIndex();
  }

  @Override
  public long position() {
    // position is relative to the start of the stream, not the buffer
    return buffer.readIndex() - startPosition;
  }

  @Override
  public int read() throws IOException {
    if (!buffer.hasRemaining()) {
    	throw new EOFException();
    }
    return buffer.get() & 0xFF; // as unsigned
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }

    int remaining = buffer.readableBytes();
    if (remaining <= 0) {
      return -1;
    }

    int bytesToRead = Math.min(buffer.readableBytes(), length);
    buffer.get(bytes, offset, bytesToRead);

    return bytesToRead;
  }

  @Override
  public long skip(long n) {
    if (n == 0) {
      return 0;
    }

    if (buffer.readableBytes() <= 0) {
      return -1;
    }

    // buffer.remaining is an int, so this will always fit in an int
    int bytesToSkip = (int) Math.min(buffer.readableBytes(), n);
    buffer.readIndex(buffer.readIndex() + bytesToSkip);

    return bytesToSkip;
  }

  @Override
  public int read(ParquetBuf out) {
    int bytesToCopy;
    ParquetBuf copyBuffer;
    if (buffer.readableBytes() <= out.writableBytes()) {
      // copy all of the buffer
      bytesToCopy = buffer.readableBytes();
      copyBuffer = buffer;
    } else {
      // copy a slice of the current buffer
      bytesToCopy = out.writableBytes();
      copyBuffer = buffer.duplicate();
      copyBuffer.writeIndex(buffer.readIndex() + bytesToCopy);
      buffer.readIndex(buffer.readIndex() + bytesToCopy);
    }

    out.put(copyBuffer);
    out.flip();

    return bytesToCopy;
  }

  @Override
  public ParquetBuf slice(int length) throws EOFException {
    if (buffer.readableBytes() < length) {
      throw new EOFException();
    }

    // length is less than remaining, so it must fit in an int
    ParquetBuf copy = buffer.duplicate();
    copy.writeIndex(copy.readIndex() + length);
    buffer.readIndex(buffer.readIndex() + length);

    return copy;
  }

  @Override
  public List<ParquetBuf> sliceBuffers(long length) throws EOFException {
    if (length == 0) {
      return Collections.emptyList();
    }

    if (length > buffer.readableBytes()) {
      throw new EOFException();
    }

    // length is less than remaining, so it must fit in an int
    return Collections.singletonList(slice((int) length));
  }

  @Override
  public List<ParquetBuf> remainingBuffers() {
    if (buffer.readableBytes() <= 0) {
      return Collections.emptyList();
    }

    ParquetBuf remaining = buffer.duplicate();
    buffer.readIndex(buffer.writeIndex());

    return Collections.singletonList(remaining);
  }

  @Override
  public void close() throws IOException {
    if (buffer instanceof AutoCloseable) {
      try {
        ((AutoCloseable) buffer).close();
      } catch (Exception e) {
        throw new IOException("Fail to close buffer", e);
      }
    }
  }

  @Override
  public void mark(int readlimit) {
    this.mark = buffer.readIndex();
  }

  @Override
  public void reset() throws IOException {
    if (mark >= 0) {
      buffer.readIndex(mark);
      this.mark = -1;
    } else {
      throw new IOException("No mark defined");
    }
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public int available() {
    return buffer.readableBytes();
  }
}
