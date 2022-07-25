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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopInputFile implements InputFile {
  private static final String MAJOR_MINOR_REGEX = "^(\\d+)\\.(\\d+)(\\..*)?$";
  private static final Pattern HADOOP3_MATCHER = Pattern.compile(MAJOR_MINOR_REGEX);

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

  public static boolean isHadoop3() {
    String version = VersionInfo.getVersion();
    return isHadoop3(version);
  }

  @VisibleForTesting
  static boolean isHadoop3(String version) {
    Matcher matcher = HADOOP3_MATCHER.matcher(version);
    if (matcher.matches()) {
      return matcher.group(1).equals("3");
    }
    return false;
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

  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public long getLength() {
    return stat.getLen();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    FSDataInputStream stream;
    try {
      if (isHadoop3()) {
        stream = fs.openFile(stat.getPath())
          .withFileStatus(stat)
          .build()
          .get();
      } else {
        stream = fs.open(stat.getPath());
      }
    } catch (Exception e) {
      throw new IOException("Error when opening file " + stat.getPath(), e);
    }
    if (stat.getPath().toString().startsWith("s3c")
      || stat.getPath().toString().startsWith("blobc")) {
      // S3AInputStream is categorized as H1SeekableInputStream
      // also McQueen RangeReadInputStream
      return new H1SeekableInputStream(stream, true);
    } else {
      return HadoopStreams.wrap(stream);
    }
  }

  @Override
  public SeekableInputStream newStream(long offset, long length) throws IOException {
    try {
      FSDataInputStream stream;
      if (isHadoop3()) {
        FutureDataInputStreamBuilder inputStreamBuilder = fs
          .openFile(stat.getPath())
          .withFileStatus(stat);

        if (stat.getPath().toString().startsWith("s3a")) {
          // Switch to random S3 input policy so that we don't do sequential read on the entire
          // S3 object. By default, the policy is normal which does sequential read until a back
          // seek happens, which in our case will never happen.
          //
          // Also set read ahead length equal to the column chunk length so we don't have to open
          // multiple S3 http connections.
          inputStreamBuilder = inputStreamBuilder
            .opt("fs.s3a.experimental.input.fadvise", "random")
            .opt("fs.s3a.readahead.range", Long.toString(length));
        }

        stream = inputStreamBuilder.build().get();
      } else {
        stream = fs.open(stat.getPath());
      }
      return HadoopStreams.wrap(stream);
    } catch (Exception e) {
      throw new IOException("Error when opening file " + stat.getPath() +
        ", offset=" + offset + ", length=" + length, e);
    }
  }

  @Override
  public String toString() {
    return stat.getPath().toString();
  }
}
