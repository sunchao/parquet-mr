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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.ExecutorService;
import org.apache.parquet.ShouldNeverHappenException;
import org.apache.parquet.io.SeekableInputStream;

public abstract class ByteBufferInputStream extends InputStream {

  /**
   * Wraps a list of 'buffers' into an input stream. All the buffers should be allocated by the
   * input 'allocator'. Once the returned input stream is closed, the 'allocator' will be used to
   * close all the buffers.
   *
   * @param allocator the original allocator that created all the buffers, will be used to
   *                  de-allocate them once the returned input stream is closed
   * @param buffers byte buffers that constitute the input stream
   * @return an input stream wrapping all the buffers
   */
  public static ByteBufferInputStream wrap(ByteBufferAllocator allocator, ByteBuffer... buffers) {
    if (buffers.length == 1) {
      return new SingleBufferInputStream(allocator, buffers[0]);
    } else {
      return new MultiBufferInputStream(Arrays.asList(buffers));
    }
  }

  /**
   * Wraps a list of 'buffers' into an input stream.
   *
   * @param buffers byte buffers that constitute the input stream
   * @return an input stream wrapping all the buffers
   */
  public static ByteBufferInputStream wrap(ByteBuffer... buffers) {
    return wrap(null, buffers);
  }

  /**
   * @see #wrap(ByteBufferAllocator, ByteBuffer...)
   */
  public static ByteBufferInputStream wrap(ByteBufferAllocator allocator,
      List<ByteBuffer> buffers) {
    if (buffers.size() == 1) {
      return new SingleBufferInputStream(allocator, buffers.get(0));
    } else {
      return new MultiBufferInputStream(buffers);
    }
  }

  /**
   * @see #wrap(ByteBuffer...)
   */
  public static ByteBufferInputStream wrap(List<ByteBuffer> buffers) {
    return wrap(null, buffers);
  }

  public static ByteBufferInputStream wrapAsync(ExecutorService threadPool,
      SeekableInputStream fileInputStream, List<ByteBuffer> buffers) {
    return new AsyncMultiBufferInputStream(threadPool, fileInputStream, buffers);
  }

  public abstract ByteBufferAllocator allocator();

  public abstract long position();

  public void skipFully(long n) throws IOException {
    long skipped = skip(n);
    if (skipped < n) {
      throw new EOFException(
          "Not enough bytes to skip: " + skipped + " < " + n);
    }
  }

  public abstract int read(ByteBuffer out);

  public abstract ByteBuffer slice(int length) throws EOFException;

  public abstract List<ByteBuffer> sliceBuffers(long length) throws EOFException;

  public ByteBufferInputStream sliceStream(long length) throws EOFException {
    return ByteBufferInputStream.wrap(allocator(), sliceBuffers(length));
  }

  public abstract List<ByteBuffer> remainingBuffers();

  public ByteBufferInputStream remainingStream() {
    return ByteBufferInputStream.wrap(remainingBuffers());
  }

  public abstract int read() throws IOException;

  public abstract int read(byte[] b, int off, int len) throws IOException;

  public abstract long skip(long n);

  public abstract int available();

  public abstract void mark(int readlimit);

  public abstract void reset() throws IOException;

  public abstract boolean markSupported();
}
