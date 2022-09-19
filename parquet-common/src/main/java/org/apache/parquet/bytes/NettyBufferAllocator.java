package org.apache.parquet.bytes;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

public class NettyBufferAllocator implements ByteBufferAllocator {
  private final ByteBufAllocator wrapped;

  public NettyBufferAllocator() {
    this.wrapped = UnpooledByteBufAllocator.DEFAULT;
  }

  @Override
  public ParquetBuf allocate(int size) {
    return ParquetBuf.fromNetty(wrapped.buffer());
  }

  @Override
  public void release(ParquetBuf b) {
    b.release();
  }

  @Override
  public boolean isDirect() {
    return wrapped.isDirectBufferPooled();
  }
}
