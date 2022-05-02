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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncMultiBufferInputStream extends MultiBufferInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncMultiBufferInputStream.class);

  final SeekableInputStream fileInputStream;
  int fetchIndex = 0;
  int readIndex = 0;
  ExecutorService threadPool;
  LinkedBlockingQueue<Future<Void>> readFutures;
  BlockingQueue<ByteBuffer> buffersRead;
  boolean closed = false;
  Exception ioException;

  LongAdder totalTimeBlocked = new LongAdder();
  LongAdder totalCountBlocked = new LongAdder();
  LongAccumulator maxTimeBlocked = new LongAccumulator(Long::max, 0L);

  AsyncMultiBufferInputStream(ExecutorService threadPool, SeekableInputStream fileInputStream,
    List<ByteBuffer> buffers) {
    super(buffers);
    this.fileInputStream = fileInputStream;
    this.threadPool = threadPool;
    readFutures = new LinkedBlockingQueue<>(buffers.size());
    buffersRead = new LinkedBlockingQueue<>(buffers.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug("ASYNC: Begin read into buffers ");
      for (ByteBuffer buf : buffers) {
        LOG.debug("ASYNC: buffer {} ", buf);
      }
    }
    fetchAll();
  }

  private void checkState() {
    if (closed) {
      throw new RuntimeException("Stream is closed");
    }
    synchronized (this) {
      if (ioException != null) {
        throw new RuntimeException(ioException);
      }
    }
  }

  private void fetchAll() {
    checkState();
    submitReadTask(0);
  }

  private void submitReadTask(int bufferNo) {
    ByteBuffer buffer = buffers.get(bufferNo);
    try {
      readFutures.put(threadPool.submit(() -> {
          readOneBuffer(buffer);
          if (bufferNo < buffers.size() - 1) {
            submitReadTask(bufferNo + 1);
          }
          return null;
        })
      );
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void readOneBuffer(ByteBuffer buffer) {
    long startTime = System.nanoTime();
    try {
      fileInputStream.readFully(buffer);
      buffer.flip();
      long readCompleted = System.nanoTime();
      long timeSpent = readCompleted - startTime;
      LOG.debug("ASYNC Stream: READ - {}", timeSpent / 1000.0);
      long putStart = System.nanoTime();
      buffersRead.put(buffer);
      long putCompleted = System.nanoTime();
      LOG.debug("ASYNC Stream: FS READ (output) BLOCKED - {}",
        (putCompleted - putStart) / 1000.0);
      fetchIndex++;
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      // Save the exception so that the calling thread can check if something went wrong.
      // checkState will throw an exception if the read task has failed.
      synchronized(this) {
        ioException = e;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean nextBuffer() {
    checkState();
    // hack: parent constructor can call this method before this class is fully initialized.
    // Just return without doing anything.
    if (buffersRead == null) {
      return false;
    }
    if (readIndex < buffers.size()) {
      long start = System.nanoTime();
      try {
        LOG.debug("ASYNC (next): Getting next buffer");
        //noinspection unused
        ByteBuffer readResult = buffersRead.take();
        long timeSpent = System.nanoTime() - start;
        totalCountBlocked.add(1);
        totalTimeBlocked.add(timeSpent);
        maxTimeBlocked.accumulate(timeSpent);
        LOG.debug("ASYNC (next): {}: Time blocked for read {} ns", this, timeSpent);
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        LOG.error("Async (next): exception while getting next buffer: ", e);
        throw new RuntimeException(e);
      }
      readIndex++;
    }
    return super.nextBuffer();
  }

  public void close() {
    LOG.debug("ASYNC Stream: Blocked: {} {} {}", totalTimeBlocked.longValue() / 1000.0,
      totalCountBlocked.longValue(), maxTimeBlocked.longValue() / 1000.0);
    Future<Void> readResult;
    while (!readFutures.isEmpty()) {
      try {
        readResult = readFutures.poll();
        readResult.get();
        if (!readResult.isDone() && !readResult.isCancelled()) {
          readResult.cancel(true);
        } else {
          readResult.get(1, TimeUnit.MILLISECONDS);
        }
      } catch (Exception e) {
        // Do nothing
      }
    }
    closed = true;
  }

}
