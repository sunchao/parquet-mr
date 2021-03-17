/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.apple.parquet.crypto.keytools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

import java.util.List;

public class AppleCryptoFactory extends PropertiesDrivenCryptoFactory {

  public static final String UNIFORM_ENCRYPTION_PROPERTY_NAME = "parquet.uniform.encryption";

  @Override
  public FileEncryptionProperties getFileEncryptionProperties(Configuration fileHadoopConfig, Path tempFilePath,
                                                              WriteSupport.WriteContext fileWriteContext) throws ParquetCryptoRuntimeException {
    if (fileHadoopConfig.getBoolean(UNIFORM_ENCRYPTION_PROPERTY_NAME, false)) {
      if (null != fileHadoopConfig.get(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME)) {
        throw new ParquetCryptoRuntimeException("Can't set both " + UNIFORM_ENCRYPTION_PROPERTY_NAME +
          " and " + PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME);
      }

      String footerKeyId = fileHadoopConfig.getTrimmed(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME);
      if (null == footerKeyId) {
        throw new ParquetCryptoRuntimeException("Footer key ID not set in uniform encryption");
      }
      String columnKeyProperty = footerKeyId + ":";
      MessageType schema = fileWriteContext.getSchema();
      List<String[]> allColumns = schema.getPaths();
      for (String[] columnPath : allColumns) {
        String columnDotPath = Strings.join(columnPath, ".");
        columnKeyProperty += columnDotPath + ",";
      }

      fileHadoopConfig.set(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, columnKeyProperty);
    }

    return super.getFileEncryptionProperties(fileHadoopConfig, tempFilePath,fileWriteContext);
  }
}
