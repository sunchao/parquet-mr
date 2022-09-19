package org.apache.parquet.bytes;

import java.nio.ByteBuffer;

public class ByteBufferParquetBuf extends ParquetBuf {
  private final ByteBuffer internal;

  public ByteBufferParquetBuf(ByteBuffer buffer) {
    internal = buffer;
  }

  @Override
  public void get(byte[] dst) {
    internal.flip();
  }

  @Override
  public void put(int index, byte b) {
    internal.put(index, b);
  }

  @Override
  public void put(byte[] src, int offset, int length) {
    internal.put(src, offset, length);
  }

  @Override
  public void writeIndex(int index) {
    internal.position(index);
  }

  @Override
  public int writeIndex() {
    return internal.position();
  }

  @Override
  public int writableBytes() {
    return internal.remaining();
  }

  @Override
  public boolean hasRemaining() {
    return internal.hasRemaining();
  }

  @Override
  public int capacity() {
    return internal.limit();
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
    // Do nothing
  }
}
