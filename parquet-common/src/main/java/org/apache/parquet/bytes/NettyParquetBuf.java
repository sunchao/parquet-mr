package org.apache.parquet.bytes;

import io.netty.buffer.ByteBuf;

public class NettyParquetBuf extends ParquetBuf {
  private ByteBuf internal;

  public NettyParquetBuf(ByteBuf buffer) {
    internal = buffer;
  }

  @Override
  public void get(byte[] dst) {
    internal.getBytes(internal.readerIndex(), dst);
  }

  @Override
  public void put(int index, byte b) {
    internal.setByte(index, b);
  }

  @Override
  public void put(byte[] b, int offset, int length) {
    int index = internal.writerIndex();
    internal.setBytes(index, b, offset, length);
    internal.writerIndex(index + length);
  }

  @Override
  public void writeIndex(int index) {
    internal = internal.writerIndex(index);
  }

  @Override
  public int writeIndex() {
    return internal.writerIndex();
  }

  @Override
  public int writableBytes() {
    return internal.writableBytes();
  }

  @Override
  public boolean hasRemaining() {
    return internal.writableBytes() > 0;
  }

  @Override
  public int capacity() {
    return internal.capacity();
  }

  @Override
  public boolean isDirect() {
    return internal.isDirect();
  }

  @Override
  public boolean hasArray() {
    return internal.hasArray();
  }

  @Override
  public byte[] array() {
    return internal.array();
  }

  @Override
  public int arrayOffset() {
    return internal.arrayOffset();
  }

  @Override
  public void release() {
    internal.release();
  }
}
