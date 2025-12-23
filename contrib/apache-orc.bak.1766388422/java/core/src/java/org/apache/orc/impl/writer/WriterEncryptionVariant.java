/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.writer;

import org.apache.orc.EncryptionVariant;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.LocalKey;
import org.jetbrains.annotations.NotNull;

import java.security.Key;
import java.util.ArrayList;
import java.util.List;

public class WriterEncryptionVariant implements EncryptionVariant {
  private int id;
  private final WriterEncryptionKey key;
  private final TypeDescription root;
  private final LocalKey material;
  private final OrcProto.FileStatistics.Builder fileStats =
      OrcProto.FileStatistics.newBuilder();
  private final List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();

  public WriterEncryptionVariant(WriterEncryptionKey key,
                                 TypeDescription root,
                                 LocalKey columnKey) {
    this.key = key;
    this.root = root;
    this.material = columnKey;
  }

  @Override
  public WriterEncryptionKey getKeyDescription() {
    return key;
  }

  @Override
  public TypeDescription getRoot() {
    return root;
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public int getVariantId() {
    return id;
  }

  @Override
  public Key getFileFooterKey() {
    return material.getDecryptedKey();
  }

  @Override
  public Key getStripeKey(long stripe) {
    return material.getDecryptedKey();
  }

  public LocalKey getMaterial() {
    return material;
  }

  public void clearFileStatistics() {
    fileStats.clearColumn();
  }

  public OrcProto.FileStatistics getFileStatistics() {
    return fileStats.build();
  }

  public void addEncoding(OrcProto.ColumnEncoding encoding) {
    encodings.add(encoding);
  }

  public List<OrcProto.ColumnEncoding> getEncodings() {
    return encodings;
  }

  public void clearEncodings() {
    encodings.clear();
  }

  @Override
  public int hashCode() {
    return key.hashCode() << 16 ^ root.getId();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other == null || other.getClass() != getClass()) {
      return false;
    }
    return compareTo((WriterEncryptionVariant) other) == 0;
  }

  @Override
  public int compareTo(@NotNull EncryptionVariant other) {
    int result = key.compareTo(other.getKeyDescription());
    if (result == 0) {
      result = Integer.compare(root.getId(), other.getRoot().getId());
    }
    return result;
  }
}

