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

package org.apache.orc.impl;

import org.apache.orc.EncryptionVariant;
import org.apache.orc.OrcProto;
import org.jetbrains.annotations.NotNull;

/**
 * The name of a stream within a stripe.
 * <p>
 * Sorted by area, encryption, column, and then kind.
 */
public class StreamName implements Comparable<StreamName> {
  private final int column;
  private final EncryptionVariant encryption;
  private final OrcProto.Stream.Kind kind;

  public enum Area {
    DATA, INDEX, FOOTER
  }

  public StreamName(int column, OrcProto.Stream.Kind kind) {
    this(column, kind, null);
  }

  public StreamName(int column, OrcProto.Stream.Kind kind,
                    EncryptionVariant encryption) {
    this.column = column;
    this.kind = kind;
    this.encryption = encryption;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof  StreamName) {
      StreamName other = (StreamName) obj;
      return other.column == column && other.kind == kind &&
                 encryption == other.encryption;
    } else {
      return false;
    }
  }

  @Override
  public int compareTo(@NotNull StreamName streamName) {
    Area area = getArea();
    Area otherArea = streamName.getArea();
    if (area != otherArea) {
      return otherArea.compareTo(area);
    } else if (encryption != streamName.encryption) {
      if (encryption == null || streamName.encryption == null) {
        return encryption == null ? -1 : 1;
      } else {
        return encryption.getVariantId() < streamName.encryption.getVariantId()?
                   -1 : 1;
      }
    } else if (column != streamName.column) {
      return column < streamName.column ? -1 : 1;
    }
    return kind.compareTo(streamName.kind);
  }

  public int getColumn() {
    return column;
  }

  public OrcProto.Stream.Kind getKind() {
    return kind;
  }

  public Area getArea() {
    return getArea(kind);
  }

  public static Area getArea(OrcProto.Stream.Kind kind) {
    switch (kind) {
      case FILE_STATISTICS:
      case STRIPE_STATISTICS:
        return Area.FOOTER;
      case ROW_INDEX:
      case DICTIONARY_COUNT:
      case BLOOM_FILTER:
      case BLOOM_FILTER_UTF8:
      case ENCRYPTED_INDEX:
        return Area.INDEX;
      default:
        return Area.DATA;
    }
  }

  /**
   * Get the encryption information for this stream.
   * @return the encryption information or null if it isn't encrypted
   */
  public EncryptionVariant getEncryption() {
    return encryption;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("column ");
    buffer.append(column);
    buffer.append(" kind ");
    buffer.append(kind);
    if (encryption != null) {
      buffer.append(" encrypt ");
      buffer.append(encryption.getKeyDescription());
    }
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    return (encryption == null ? 0 : encryption.getVariantId() * 10001) +
               column * 101 + kind.getNumber();
  }
}

