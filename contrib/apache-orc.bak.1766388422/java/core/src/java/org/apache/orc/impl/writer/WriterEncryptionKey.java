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

import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.EncryptionKey;
import org.apache.orc.impl.HadoopShims;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WriterEncryptionKey implements EncryptionKey {
  private final HadoopShims.KeyMetadata metadata;
  private final List<WriterEncryptionVariant> roots = new ArrayList<>();
  private int id;

  public WriterEncryptionKey(HadoopShims.KeyMetadata key) {
    this.metadata = key;
  }

  public void addRoot(WriterEncryptionVariant root) {
    roots.add(root);
  }

  public HadoopShims.KeyMetadata getMetadata() {
    return metadata;
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public String getKeyName() {
    return metadata.getKeyName();
  }

  @Override
  public int getKeyVersion() {
    return metadata.getVersion();
  }

  @Override
  public EncryptionAlgorithm getAlgorithm() {
    return metadata.getAlgorithm();
  }

  @Override
  public WriterEncryptionVariant[] getEncryptionRoots() {
    return roots.toArray(new WriterEncryptionVariant[0]);
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  public int getId() {
    return id;
  }

  public void sortRoots() {
    Collections.sort(roots);
  }

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    return compareTo((EncryptionKey) other) == 0;
  }

  @Override
  public int compareTo(@NotNull EncryptionKey other) {
    int result = getKeyName().compareTo(other.getKeyName());
    if (result == 0) {
      result = Integer.compare(getKeyVersion(), other.getKeyVersion());
    }
    return result;
  }

  @Override
  public String toString() {
    return metadata.toString();
  }
}
