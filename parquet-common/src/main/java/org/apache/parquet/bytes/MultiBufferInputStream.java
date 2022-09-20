/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.bytes;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class MultiBufferInputStream extends ByteBufferInputStream {
  private static final ParquetBuf EMPTY = new ByteBufferParquetBuf(ByteBuffer.allocate(0));

  protected final List<ParquetBuf> buffers;
  private final long length;

  private Iterator<ParquetBuf> iterator;
  private ParquetBuf current = EMPTY;
  private long position = 0;

  private long mark = -1;
  private long markLimit = 0;
  private List<ParquetBuf> markBuffers = new ArrayList<>();

  @Override
  public String toString() {
    return "MultiBufferInputStream{" +
      "length=" + length +
      ", current=" + current +
      ", position=" + position +
      '}';
  }

  MultiBufferInputStream(List<ParquetBuf> buffers) {
    this.buffers = buffers;

    long totalLen = 0;
    for (ParquetBuf buffer : buffers) {
      totalLen += buffer.readableBytes();
    }
    this.length = totalLen;

    this.iterator = buffers.iterator();

    nextBuffer();
  }

  /**
   * Returns the position in the stream.
   */
  public long position() {
    return position;
  }

  @Override
  public long skip(long n) {
    if (n <= 0) {
      return 0;
    }

    if (current == null) {
      return -1;
    }

    long bytesSkipped = 0;
    while (bytesSkipped < n) {
      if (current.readableBytes() > 0) {
        long bytesToSkip = Math.min(n - bytesSkipped, current.readableBytes());
        current.readIndex(current.readIndex() + (int) bytesToSkip);
        bytesSkipped += bytesToSkip;
        this.position += bytesToSkip;
      } else if (!nextBuffer()) {
        // there are no more buffers
        return bytesSkipped > 0 ? bytesSkipped : -1;
      }
    }

    return bytesSkipped;
  }

  @Override
  public int read(ParquetBuf out) {
    int len = out.readableBytes();
    if (len <= 0) {
      return 0;
    }

    if (current == null) {
      return -1;
    }

    int bytesCopied = 0;
    while (bytesCopied < len) {
      if (current.readableBytes() > 0) {
        int bytesToCopy;
        ParquetBuf copyBuffer;
        if (current.readableBytes() <= out.writableBytes()) {
          // copy all of the current buffer
          bytesToCopy = current.readableBytes();
          copyBuffer = current;
        } else {
          // copy a slice of the current buffer
          bytesToCopy = out.writableBytes();
          copyBuffer = current.duplicate();
          copyBuffer.writeIndex(copyBuffer.readIndex() + bytesToCopy);
          current.readIndex(copyBuffer.readIndex() + bytesToCopy);
        }

        out.put(copyBuffer);
        bytesCopied += bytesToCopy;
        this.position += bytesToCopy;

      } else if (!nextBuffer()) {
        // there are no more buffers
        return bytesCopied > 0 ? bytesCopied : -1;
      }
    }

    return bytesCopied;
  }

  @Override
  public ParquetBuf slice(int length) throws EOFException {
    if (length <= 0) {
      return EMPTY;
    }

    if (current == null) {
      throw new EOFException();
    }

    ParquetBuf slice;
    if (length > current.readableBytes()) {
      // a copy is needed to return a single buffer
      // TODO: use an allocator
      slice = ParquetBuf.allocate(length);
      int bytesCopied = read(slice);
      slice.flip();
      if (bytesCopied < length) {
        throw new EOFException();
      }
    } else {
      slice = current.duplicate();
      slice.writeIndex(slice.readIndex() + length);
      current.readIndex(slice.readIndex() + length);
      this.position += length;
    }

    return slice;
  }

  public List<ParquetBuf> sliceBuffers(long len) throws EOFException {
    if (len <= 0) {
      return Collections.emptyList();
    }

    if (current == null) {
      throw new EOFException();
    }

    List<ParquetBuf> buffers = new ArrayList<>();
    long bytesAccumulated = 0;
    while (bytesAccumulated < len) {
      if (current.readableBytes() > 0) {
        // get a slice of the current buffer to return
        // always fits in an int because remaining returns an int that is >= 0
        int bufLen = (int) Math.min(len - bytesAccumulated, current.readableBytes());
        ParquetBuf slice = current.duplicate();
        slice.writeIndex(slice.readIndex() + bufLen);
        buffers.add(slice);
        bytesAccumulated += bufLen;

        // update state; the bytes are considered read
        current.readIndex(current.readIndex() + bufLen);
        this.position += bufLen;
      } else if (!nextBuffer()) {
        // there are no more buffers
        throw new EOFException();
      }
    }

    return buffers;
  }

  @Override
  public List<ParquetBuf> remainingBuffers() {
    if (position >= length) {
      return Collections.emptyList();
    }

    try {
      return sliceBuffers(length - position);
    } catch (EOFException e) {
      throw new RuntimeException(
          "[Parquet bug] Stream is bad: incorrect bytes remaining " +
              (length - position));
    }
  }

  @Override
  public int read(byte[] bytes, int off, int len) {
    if (len <= 0) {
      if (len < 0) {
        throw new IndexOutOfBoundsException("Read length must be greater than 0: " + len);
      }
      return 0;
    }

    if (current == null) {
      return -1;
    }

    int bytesRead = 0;
    while (bytesRead < len) {
      if (current.readableBytes() > 0) {
        int bytesToRead = Math.min(len - bytesRead, current.readableBytes());
        current.get(bytes, off + bytesRead, bytesToRead);
        bytesRead += bytesToRead;
        this.position += bytesToRead;
      } else if (!nextBuffer()) {
        // there are no more buffers
        return bytesRead > 0 ? bytesRead : -1;
      }
    }

    return bytesRead;
  }

  @Override
  public int read(byte[] bytes) {
    return read(bytes, 0, bytes.length);
  }

  @Override
  public int read() throws IOException {
    if (current == null) {
      throw new EOFException();
    }

    while (true) {
      if (current.readableBytes() > 0) {
        this.position += 1;
        return current.get() & 0xFF; // as unsigned
      } else if (!nextBuffer()) {
        // there are no more buffers
        throw new EOFException();
      }
    }
  }

  @Override
  public int available() {
    long remaining = length - position;
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) remaining;
    }
  }

  @Override
  public void close() throws IOException {
    for (ParquetBuf buffer : buffers) {
      buffer.release();
    }
  }

  @Override
  public void mark(int readlimit) {
    if (mark >= 0) {
      discardMark();
    }
    this.mark = position;
    this.markLimit = mark + readlimit + 1;
    if (current != null) {
      markBuffers.add(current.duplicate());
    }
  }

  @Override
  public void reset() throws IOException {
    if (mark >= 0 && position < markLimit) {
      this.position = mark;
      // replace the current iterator with one that adds back the buffers that
      // have been used since mark was called.
      this.iterator = concat(markBuffers.iterator(), iterator);
      discardMark();
      nextBuffer(); // go back to the marked buffers
    } else {
      throw new IOException("No mark defined or has read past the previous mark limit");
    }
  }

  private void discardMark() {
    this.mark = -1;
    this.markLimit = 0;
    markBuffers = new ArrayList<>();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  protected boolean nextBuffer() {
    if (!iterator.hasNext()) {
      this.current = null;
      return false;
    }

    this.current = iterator.next().duplicate();

    if (mark >= 0) {
      if (position < markLimit) {
        // the mark is defined and valid. save the new buffer
        markBuffers.add(current.duplicate());
      } else {
        // the mark has not been used and is no longer valid
        discardMark();
      }
    }

    return true;
  }

  private static <E> Iterator<E> concat(Iterator<E> first, Iterator<E> second) {
    return new ConcatIterator<>(first, second);
  }

  private static class ConcatIterator<E> implements Iterator<E> {
    private final Iterator<E> first;
    private final Iterator<E> second;
    boolean useFirst = true;

    public ConcatIterator(Iterator<E> first, Iterator<E> second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public boolean hasNext() {
      if (useFirst) {
        if (first.hasNext()) {
          return true;
        } else {
          useFirst = false;
          return second.hasNext();
        }
      }
      return second.hasNext();
    }

    @Override
    public E next() {
      if (useFirst && !first.hasNext()) {
        useFirst = false;
      }

      if (!useFirst && !second.hasNext()) {
        throw new NoSuchElementException();
      }

      if (useFirst) {
        return first.next();
      }

      return second.next();
    }

    @Override
    public void remove() {
      if (useFirst) {
        first.remove();
      }
      second.remove();
    }
  }
}
