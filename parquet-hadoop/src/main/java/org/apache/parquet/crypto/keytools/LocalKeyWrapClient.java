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

package org.apache.parquet.crypto.keytools;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.crypto.KeyAccessDeniedException;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Typically, KMS systems support in-server key wrapping. Their clients should implement KmsClient interface directly.
 * An extension of the LocalKeyWrapClient class should used only in situations where in-server wrapping is not
 * supported. The wrapping will be done locally then - the master keys will be fetched from the KMS server via the
 * getMasterKey functions, and used to encrypt a DEK or KEK by AES GCM cipher.
 *
 * Master keys have a version, updated upon key rotation in KMS. In the read path, the master keys are fetched by the
 * key ID and version, and cached locally in this class - so they can be re-used for reading different files without
 * additional KMS interactions. In the write path, the master keys are fetched by the key ID only - the assumption is
 * that KMS will send the current (latest) version of the key; therefore the keys are not cached in this class -
 * writing new files will be done with the latest key version, sent from the KMS. However, the classes that extend the
 * LocalKeyWrapClient class, can decide to cache the master keys in the write path, and handle key rotation manually.
 */
public abstract class LocalKeyWrapClient implements KmsClient {

  public static final String LOCAL_WRAP_TYPE_FIELD = "localWrappingType";
  public static final String LOCAL_WRAP_TYPE1 = "LKW1";
  public static final String LOCAL_WRAP_MASTER_KEY_VERSION_FIELD = "masterKeyVersion";
  public static final String LOCAL_WRAP_ENCRYPTED_KEY_FIELD = "encryptedKey";

  protected String kmsInstanceID;
  protected String kmsInstanceURL;
  protected String kmsToken;
  protected Configuration hadoopConfiguration;

  // MasterKey cache for read path: master keys per (key ID + ":" + key version)
  protected ConcurrentMap<String, byte[]> masterKeyCache;

  public static class MasterKeyWithVersion {
    private final byte[] masterKey;
    private final String masterKeyVersion;

    public MasterKeyWithVersion(byte[] masterKey, String masterKeyVersion) {
      this.masterKey = masterKey;
      this.masterKeyVersion = masterKeyVersion;
    }

    private byte[] getKey() {
      return masterKey;
    }

    private String getVersion() {
      return masterKeyVersion;
    }
  }

  /**
   * KMS systems wrap keys by encrypting them by master keys, and attaching additional information (such as the version
   * number of the masker key) to the result of encryption. The master key version is required for key rotation.
   *
   * LocalKeyWrapClient class writes (and reads) the "key wrap information" as a flat json with the following fields:
   * 1. "localWrappingType" - a String, with the type of  key material. In the current version, only one value is
   *     allowed - "LKW1" (stands for "local key wrapping, version 1")
   * 2. "masterKeyVersion" - a String, with the master key version.
   * 3. "encryptedKey" - a String, with the key encrypted by the master key (base64-encoded).
   */
  private static class LocalKeyWrap {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String encryptedEncodedKey;
    private final String masterKeyVersion;

    private LocalKeyWrap(String masterKeyVersion, String encryptedEncodedKey) {
      this.masterKeyVersion = masterKeyVersion;
      this.encryptedEncodedKey = encryptedEncodedKey;
    }

    private static String createSerialized(String encryptedEncodedKey, String masterKeyVersion) {
      Map<String, String> keyWrapMap = new HashMap<>(3);
      keyWrapMap.put(LOCAL_WRAP_TYPE_FIELD, LOCAL_WRAP_TYPE1);
      keyWrapMap.put(LOCAL_WRAP_MASTER_KEY_VERSION_FIELD, masterKeyVersion);
      keyWrapMap.put(LOCAL_WRAP_ENCRYPTED_KEY_FIELD, encryptedEncodedKey);
      try {
        return OBJECT_MAPPER.writeValueAsString(keyWrapMap);
      } catch (IOException e) {
        throw new ParquetCryptoRuntimeException("Failed to serialize local key wrap map", e);
      }
    }

    private static LocalKeyWrap parse(String wrappedKey) {
      Map<String, String> keyWrapMap;
      try {
        keyWrapMap = OBJECT_MAPPER.readValue(new StringReader(wrappedKey),
          new TypeReference<Map<String, String>>() {
          });
      } catch (IOException e) {
        throw new ParquetCryptoRuntimeException("Failed to parse local key wrap json " + wrappedKey, e);
      }
      String localWrappingType = keyWrapMap.get(LOCAL_WRAP_TYPE_FIELD);
      String masterKeyVersion = keyWrapMap.get(LOCAL_WRAP_MASTER_KEY_VERSION_FIELD);
      if (null == localWrappingType) {
        // Check for files with initial experimental version
        if (!LocalWrapKmsClient.LOCAL_WRAP_NO_KEY_VERSION.equals(masterKeyVersion)) {
          throw new ParquetCryptoRuntimeException("No localWrappingType defined for key version: " + masterKeyVersion);
        }
      } else if (!LOCAL_WRAP_TYPE1.equals(localWrappingType)) {
        throw new ParquetCryptoRuntimeException("Unsupported localWrappingType: " + localWrappingType);
      }
      String encryptedEncodedKey = keyWrapMap.get(LOCAL_WRAP_ENCRYPTED_KEY_FIELD);

      return new LocalKeyWrap(masterKeyVersion, encryptedEncodedKey);
    }

    private String getMasterKeyVersion() {
      return masterKeyVersion;
    }

    private String getEncryptedKey() {
      return encryptedEncodedKey;
    }
  }

  @Override
  public void initialize(Configuration configuration, String kmsInstanceID, String kmsInstanceURL, String accessToken) {
    this.kmsInstanceID = kmsInstanceID;
    this.kmsInstanceURL = kmsInstanceURL;

    masterKeyCache = new ConcurrentHashMap<>();
    hadoopConfiguration = configuration;
    kmsToken = accessToken;

    initializeInternal();
  }

  @Override
  public String wrapKey(byte[] key, String masterKeyIdentifier) throws KeyAccessDeniedException {
    // refresh token
    kmsToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME);
    MasterKeyWithVersion masterKeyLastVersion = getMasterKey(masterKeyIdentifier);
    checkMasterKeyLength(key.length, masterKeyIdentifier, masterKeyLastVersion.getVersion());
    String encryptedEncodedKey =  KeyToolkit.encryptKeyLocally(key, masterKeyLastVersion.getKey(), null);
    return LocalKeyWrap.createSerialized(encryptedEncodedKey, masterKeyLastVersion.getVersion());
  }

  @Override
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier) throws KeyAccessDeniedException {
    LocalKeyWrap keyWrap = LocalKeyWrap.parse(wrappedKey);
    String masterKeyVersionedID = masterKeyIdentifier + ":" + keyWrap.getMasterKeyVersion();
    String encryptedEncodedKey = keyWrap.getEncryptedKey();
    byte[] masterKey = masterKeyCache.computeIfAbsent(masterKeyVersionedID,
        (k) -> getMasterKeyForVersion(masterKeyIdentifier, keyWrap.getMasterKeyVersion()));
    return KeyToolkit.decryptKeyLocally(encryptedEncodedKey, masterKey, null);
  }

  private byte[] getMasterKeyForVersion(String keyIdentifier, String keyVersion) {
    // refresh token
    kmsToken = hadoopConfiguration.getTrimmed(KeyToolkit.KEY_ACCESS_TOKEN_PROPERTY_NAME);
    byte[] key = getMasterKey(keyIdentifier, keyVersion);
    checkMasterKeyLength(key.length, keyIdentifier, keyVersion);

    return key;
  }

  private void checkMasterKeyLength(int keyLength, String keyID, String keyVersion) {
    if (!(16 == keyLength || 24 == keyLength || 32 == keyLength)) {
      throw new ParquetCryptoRuntimeException( "Wrong length: "+ keyLength +
        " of master key: "  + keyID + ", version: " +  keyVersion);
    }
  }

  /**
   * Write path: Get the current (latest) version of the master key
   *
   * @param masterKeyIdentifier: a string that identifies the master key
   * @return current version of the master key
   * @throws KeyAccessDeniedException unauthorized to get the master key
   */
  protected abstract MasterKeyWithVersion getMasterKey(String masterKeyIdentifier)
      throws KeyAccessDeniedException;

  /**
   * Read path: Get master key with a given version
   *
   * @param masterKeyIdentifier: a string that identifies the master key
   * @param masterKeyVersion: a string that specifies the master key version
   * @return master key bytes
   * @throws KeyAccessDeniedException unauthorized to get the master key
   */
  protected abstract byte[] getMasterKey(String masterKeyIdentifier, String masterKeyVersion)
    throws KeyAccessDeniedException;

  /**
   * Pass configuration with KMS-specific parameters.
   */
  protected abstract void initializeInternal()
      throws KeyAccessDeniedException;
}
