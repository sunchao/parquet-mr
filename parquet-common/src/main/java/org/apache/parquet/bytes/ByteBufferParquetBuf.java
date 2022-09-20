package org.apache.parquet.bytes;

import java.nio.ByteBuffer;

public class ByteBufferParquetBuf implements ParquetBuf {
  private final ByteBuffer internal;

  public ByteBufferParquetBuf(ByteBuffer buffer) {
    internal = buffer;
  }

  @Override
  public ByteBuffer toByteBuffer() {
    return internal;
  }

  @Override
  public byte get() {
    return internal.get();
  }

  @Override
  public void get(byte[] dst) {
    internal.get(dst);
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    internal.get(dst, offset, length);
  }

  @Override
  public void put(byte b) {
    internal.put(b);
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
  public void put(ParquetBuf buf) {
    internal.put(buf.toByteBuffer());
  }

  @Override
  public void writeIndex(int index) {
    internal.position(index);
  }

  @Override
  public int readIndex() {
    return internal.position();
  }

  @Override
  public void readIndex(int index) {
    internal.position(index);
  }

  @Override
  public int readableBytes() {
    return internal.remaining();
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
  public void flip() {
    internal.flip();
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
  public ParquetBuf duplicate() {
    return new ByteBufferParquetBuf(internal.duplicate());
  }

  @Override
  public void release() {
    // Do nothing
  }
}
