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

package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class HadoopInputFile implements InputFile {

  private final FileSystem fs;
  private final FileStatus stat;
  private final Configuration conf;

  public static HadoopInputFile fromPath(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopInputFile(fs, fs.getFileStatus(path), conf);
  }

  public static HadoopInputFile fromStatus(FileStatus stat, Configuration conf)
      throws IOException {
    FileSystem fs = stat.getPath().getFileSystem(conf);
    return new HadoopInputFile(fs, stat, conf);
  }

  private HadoopInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
    this.fs = fs;
    this.stat = stat;
    this.conf = conf;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public Path getPath() {
    return stat.getPath();
  }

  @Override
  public long getLength() {
    return stat.getLen();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    FSDataInputStream stream;
    try {
      stream = fs.openFile(stat.getPath())
        .withFileStatus(stat)
        .build()
        .get();
    } catch (Exception e) {
      throw new IOException("Error when opening file " + stat.getPath(), e);
    }
    if (stat.getPath().toString().startsWith("s3c")
      || stat.getPath().toString().startsWith("blobc") ) {
      // S3AInputStream is categorized as H1SeekableInputStream
      // also McQueen RangeReadInputStream
      return new H1SeekableInputStream(stream, true);
    } else {
      return HadoopStreams.wrap(stream);
    }
  }

  @Override
  public String toString() {
    return stat.getPath().toString();
  }
}
