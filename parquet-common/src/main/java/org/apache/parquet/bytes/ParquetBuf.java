package org.apache.parquet.bytes;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public abstract class ParquetBuf {
  public static ParquetBuf fromByteBuffer(ByteBuffer buffer) {
    return new ByteBufferParquetBuf(buffer);
  }

  public static ParquetBuf fromNetty(ByteBuf buffer) {
    return new NettyParquetBuf(buffer);
  }

  public abstract void get(byte[] dst);

  public abstract void put(int index, byte b);

  public abstract void put(byte[] b, int offset, int length);

  public abstract void writeIndex(int index);

  public abstract int writeIndex();

  public abstract int writableBytes();

  public abstract boolean hasRemaining();

  public abstract int capacity();

  /**
   * Whether this buffer is backed by direct off-heap memory.
   */
  public abstract boolean isDirect();

  /**
   * Whether or not this buffer is backed by an accessible byte array.
   */
  public abstract boolean hasArray();

  public abstract byte[] array();

  public abstract int arrayOffset();

  /**
   * Release this buffer, including the memory it holds.
   */
  public abstract void release();

}
