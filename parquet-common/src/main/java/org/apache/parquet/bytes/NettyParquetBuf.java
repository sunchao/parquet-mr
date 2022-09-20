package org.apache.parquet.bytes;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class NettyParquetBuf implements ParquetBuf {
  private ByteBuf internal;

  public NettyParquetBuf(ByteBuf buffer) {
    internal = buffer;
  }

  @Override
  public ByteBuffer toByteBuffer() {
    return internal.nioBuffer();
  }

  @Override
  public byte get() {
    int index = internal.readerIndex();
    byte result = internal.getByte(index);
    internal.readerIndex(index + 1);
    return result;
  }

  @Override
  public void get(byte[] dst) {
    int index = internal.readerIndex();
    internal.getBytes(index, dst);
    internal.readerIndex(index + dst.length);
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    internal.getBytes(internal.readerIndex(), dst, offset, length);
  }

  @Override
  public void put(byte b) {
    internal.setByte(internal.writerIndex(), b);
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
  public void put(ParquetBuf buf) {
    if (buf instanceof NettyParquetBuf) {
      internal.writeBytes(((NettyParquetBuf) buf).internal);
    } else if (buf instanceof ByteBufferParquetBuf) {
      internal.writeBytes(buf.toByteBuffer());
    } else {
      throw new IllegalArgumentException("Invalid ParquetBuf class: " +
        buf.getClass().getSimpleName());
    }
  }

  @Override
  public void writeIndex(int index) {
    internal = internal.writerIndex(index);
  }

  @Override
  public int readIndex() {
    return internal.readerIndex();
  }

  @Override
  public void readIndex(int index) {
    internal.readerIndex(index);
  }

  @Override
  public int readableBytes() {
    return internal.readableBytes();
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
  public void flip() {
    int tmp = internal.readerIndex();
    internal.readerIndex(internal.writerIndex());
    internal.writerIndex(tmp);
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
    return new NettyParquetBuf(internal.duplicate());
  }

  @Override
  public void release() {
    internal.release();
  }
}
