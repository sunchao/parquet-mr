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
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.keytools.LocalKeyWrapClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CustomerKmsBridge extends LocalKeyWrapClient {
  private static final Logger LOG = LoggerFactory.getLogger(CustomerKmsBridge.class);

  public static final String REQUIRE_ENV_KEY_PASSING__PROPERTY_NAME = "parquet.encryption.require.environment.keys";
  public static final String VERSIONED_KEY_LIST_HADOOP_PROPERTY_NAME = "parquet.encryption.versioned.key.list";
  public static final String VERSIONED_KEY_LIST_ENV_PROPERTY_NAME = "PARQUET_ENCRYPTION_VERSIONED_KEY_LIST";
  public static final String KEY_LIST_HADOOP_PROPERTY_NAME = "parquet.encryption.key.list";
  public static final String KEY_FILE_HADOOP_PROPERTY_NAME = "parquet.encryption.key.file";

  private static ConcurrentMap<String, ConcurrentMap<String, byte[]>> masterKeyHistoryMap;
  private static ConcurrentMap<String, byte[]> latestMasterKeyMap = new ConcurrentHashMap<>();
  private static int latestKeyVersion = -1;
  private static String latestKeyVersionString;
  private static boolean versionedKeys;


  @Override
  protected MasterKeyWithVersion getMasterKey(String masterKeyIdentifier) throws KeyAccessDeniedException {
    // Always use the latest key version for writing
    byte[] masterKey = latestMasterKeyMap.get(masterKeyIdentifier);
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier +
        ". Key version: " + latestKeyVersion);
    }

    return new MasterKeyWithVersion(masterKey, latestKeyVersionString);
  }

  @Override
  protected byte[] getMasterKey(String masterKeyIdentifier, String masterKeyVersion) throws KeyAccessDeniedException {
    Map<String, byte[]> masterKeyMap = masterKeyHistoryMap.get(masterKeyVersion);
    if (null == masterKeyMap) {
      throw new ParquetCryptoRuntimeException("Key version not found: " + masterKeyVersion);
    }
    byte[] masterKey = masterKeyMap.get(masterKeyIdentifier);
    if (null == masterKey) {
      throw new ParquetCryptoRuntimeException("Key not found: " + masterKeyIdentifier);
    }
    return masterKey;
  }

  @Override
  protected synchronized void initializeInternal() throws KeyAccessDeniedException {
    String[] masterKeys = ingestCustomerKeys(hadoopConfiguration);
    masterKeyHistoryMap = new ConcurrentHashMap<>();
    parseKeyList(masterKeys);
  }

  /**
   * Fetches new key version (plus old ones for decryption).
   * Note: Using environment property for keys update might be a problem, because
   * environment is typically loaded at process start-up, and its properties
   * are hard to update during process runtime. Hadoop parameters or direct file
   * reading are better suited for this purpose.
   * <p>
   * Don't run it in parallel with writing or reading files
   */
  public static synchronized void updateMasterKeys(Configuration configuration) {
    String[] masterKeys = ingestCustomerKeys(configuration);
    masterKeyHistoryMap.clear();
    parseKeyList(masterKeys);
  }

  // Override with custom ingestion if needed
  protected static String[] ingestCustomerKeys(Configuration configuration) {
    versionedKeys = true;

    // Get master  keys
    // First, try to find them in environment
    boolean requireEnvPassing = configuration.getBoolean(REQUIRE_ENV_KEY_PASSING__PROPERTY_NAME, false);
    if (requireEnvPassing) {
      String envMasterKeyList = System.getenv(VERSIONED_KEY_LIST_ENV_PROPERTY_NAME);
      if (null == envMasterKeyList) {
        throw new ParquetCryptoRuntimeException("No encryption key list in environment parameters");
      }
      // split by ",", remove whitespaces
      return envMasterKeyList.split("\\s*,\\s*");
    }

    // Second, try Hadoop parameters
    String[] masterKeys = configuration.getTrimmedStrings(VERSIONED_KEY_LIST_HADOOP_PROPERTY_NAME);
    if (null != masterKeys && masterKeys.length > 0) {
      return masterKeys;
    }

    // Third, try to read keys from file
    String filePath = configuration.getTrimmed(KEY_FILE_HADOOP_PROPERTY_NAME);
    if (null != filePath) {
      try {
        BufferedReader fileReader = new BufferedReader(new FileReader(filePath));
        String fileMasterKeyList = "";
        for (String line; (line = fileReader.readLine()) != null; ) {
          fileMasterKeyList += line;
        }
        return fileMasterKeyList.split("\\s*,\\s*");
      } catch (IOException e) {
        throw new ParquetCryptoRuntimeException("Failed to read keys from file " + filePath, e);
      }
    }

    // Lastly, try unversioned keys -  demo only
    versionedKeys = false;
    masterKeys = configuration.getTrimmedStrings(KEY_LIST_HADOOP_PROPERTY_NAME);
    if (null == masterKeys || masterKeys.length == 0) {
      throw new ParquetCryptoRuntimeException("Key list is not found in either Hadoop, " +
        "environment parameters or in file");
    }
    return masterKeys;
  }

  private static void parseKeyList(String[] masterKeys) {
    for (String masterKey : masterKeys) {
      String[] parts = masterKey.split(":");
      String keyName = parts[0].trim();
      if (versionedKeys) {
        if (parts.length != 3) {
          throw new IllegalArgumentException("Versioned key " + masterKey + " is not formatted correctly");
        }
        String keyVersion = parts[1].trim();
        if (!keyVersion.startsWith("v")) {
          throw new IllegalArgumentException("Key version " + keyVersion + " is not formatted correctly");
        }
        int version;
        try {
          version = Integer.parseInt(keyVersion.substring(1));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Failed to parse " + keyVersion.substring(1), e);
        }
        if (version > latestKeyVersion) {
          latestKeyVersion = version;
        }
        Map<String, byte[]> keyMap = masterKeyHistoryMap.computeIfAbsent(keyVersion, k -> new ConcurrentHashMap<>());
        String key = parts[2].trim();
        byte[] keyBytes = Base64.getDecoder().decode(key);
        keyMap.put(keyName, keyBytes);

      } else { // unversioned keys
        if (parts.length != 2) {
          throw new IllegalArgumentException("Unversioned key " + parts[0] + " is not formatted correctly");
        }
        String key = parts[1].trim();
        byte[] keyBytes = Base64.getDecoder().decode(key);
        if (null == latestMasterKeyMap) {
          latestMasterKeyMap = new ConcurrentHashMap<>();
        }
        latestMasterKeyMap.put(keyName, keyBytes);
      }
    }

    latestKeyVersionString = "v" + latestKeyVersion;
    if (versionedKeys) {
      latestMasterKeyMap = masterKeyHistoryMap.get(latestKeyVersionString);
    } else {
      masterKeyHistoryMap.put(latestKeyVersionString, latestMasterKeyMap);
    }
  }
}
