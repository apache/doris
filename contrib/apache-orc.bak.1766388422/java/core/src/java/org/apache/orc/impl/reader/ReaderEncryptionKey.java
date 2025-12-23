/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.reader;

import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.EncryptionKey;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.HadoopShims;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This tracks the keys for reading encrypted columns.
 */
public class ReaderEncryptionKey implements EncryptionKey {
  private final String name;
  private final int version;
  private final EncryptionAlgorithm algorithm;
  private final List<ReaderEncryptionVariant> roots = new ArrayList<>();

  /**
   * Store the state of whether we've tried to decrypt a local key using this
   * key or not. If it fails the first time, we assume the user doesn't have
   * permission and move on. However, we don't want to retry the same failed
   * key over and over again.
   */
  public enum State {
    UNTRIED,
    FAILURE,
    SUCCESS
  }

  private State state = State.UNTRIED;

  public ReaderEncryptionKey(OrcProto.EncryptionKey key) {
    name = key.getKeyName();
    version = key.getKeyVersion();
    algorithm =
        EncryptionAlgorithm.fromSerialization(key.getAlgorithm().getNumber());
  }

  @Override
  public String getKeyName() {
    return name;
  }

  @Override
  public int getKeyVersion() {
    return version;
  }

  @Override
  public EncryptionAlgorithm getAlgorithm() {
    return algorithm;
  }

  @Override
  public ReaderEncryptionVariant[] getEncryptionRoots() {
    return roots.toArray(new ReaderEncryptionVariant[0]);
  }

  public HadoopShims.KeyMetadata getMetadata() {
    return new HadoopShims.KeyMetadata(name, version, algorithm);
  }

  public State getState() {
    return state;
  }

  public void setFailure() {
    state = State.FAILURE;
  }

  public void setSuccess() {
    if (state == State.FAILURE) {
      throw new IllegalStateException("Key " + name + " had already failed.");
    }
    state = State.SUCCESS;
  }

  void addVariant(ReaderEncryptionVariant newVariant) {
    roots.add(newVariant);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || getClass() != other.getClass()) {
      return false;
    } else if (other == this) {
      return true;
    } else {
      return compareTo((EncryptionKey) other) == 0;
    }
  }

  @Override
  public int hashCode() {
    return name.hashCode() * 127 + version * 7 + algorithm.hashCode();
  }

  @Override
  public int compareTo(@NotNull EncryptionKey other) {
    int result = name.compareTo(other.getKeyName());
    if (result == 0) {
      result = Integer.compare(version, other.getKeyVersion());
    }
    return result;
  }

  @Override
  public String toString() {
    return name + "@" + version + " w/ " + algorithm;
  }

  @Override
  public boolean isAvailable() {
    if (getState() == ReaderEncryptionKey.State.SUCCESS) {
      return true;
    } else if (getState() == ReaderEncryptionKey.State.UNTRIED &&
               roots.size() > 0) {
      // Check to see if we can decrypt the footer key of the first variant.
      try {
        return roots.get(0).getFileFooterKey() != null;
      } catch (IOException ioe) {
        setFailure();
      }
    }
    return false;
  }
}
