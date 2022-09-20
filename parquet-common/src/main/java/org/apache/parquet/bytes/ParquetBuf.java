package org.apache.parquet.bytes;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public interface ParquetBuf {
  static ParquetBuf fromByteBuffer(ByteBuffer buffer) {
    return new ByteBufferParquetBuf(buffer);
  }

  static ParquetBuf fromNetty(ByteBuf buffer) {
    return new NettyParquetBuf(buffer);
  }

  static ParquetBuf allocate(int capacity) {
    return new ByteBufferParquetBuf(ByteBuffer.allocate(capacity));
  }

  ByteBuffer toByteBuffer();

  byte get();

  void get(byte[] dst);

  void get(byte[] dst, int offset, int length);

  void put(byte b);

  void put(int index, byte b);

  void put(byte[] b, int offset, int length);

  void put(ParquetBuf buf);

  int readIndex();

  void readIndex(int index);

  int readableBytes();

  int writeIndex();

  void writeIndex(int index);

  int writableBytes();

  boolean hasRemaining();

  int capacity();

  void flip();

  /**
   * Whether this buffer is backed by direct off-heap memory.
   */
  boolean isDirect();

  /**
   * Whether or not this buffer is backed by an accessible byte array.
   */
  boolean hasArray();

  byte[] array();

  int arrayOffset();

  ParquetBuf duplicate();

  /**
   * Release this buffer, including the memory it holds.
   */
  void release();
}
