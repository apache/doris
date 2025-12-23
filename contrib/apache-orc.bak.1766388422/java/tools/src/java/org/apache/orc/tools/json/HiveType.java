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

package org.apache.orc.tools.json;

import org.apache.orc.TypeDescription;

import java.io.PrintStream;

/**
 * The internal representation of what we have discovered about a given
 * field's type.
 */
abstract class HiveType {
  enum Kind {
    NULL(0),
    BOOLEAN(1),
    BYTE(1), SHORT(2), INT(3), LONG(4), DECIMAL(5), FLOAT(6), DOUBLE(7),
    BINARY(1), DATE(1), TIMESTAMP(1), TIMESTAMP_INSTANT(1), STRING(2),
    STRUCT(1, false),
    LIST(1, false),
    UNION(8, false),
    MAP(9, false);

    // for types that subsume each other, establish a ranking.
    final int rank;
    final boolean isPrimitive;
    Kind(int rank, boolean isPrimitive) {
      this.rank = rank;
      this.isPrimitive = isPrimitive;
    }
    Kind(int rank) {
      this(rank, true);
    }
  }

  protected Kind kind;

  HiveType(Kind kind) {
    this.kind = kind;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }
    return ((HiveType) other).kind.equals(kind);
  }

  @Override
  public int hashCode() {
    return kind.hashCode();
  }

  /**
   * Does this type include all of the values of the other type?
   * @param other the other type to compare against
   * @return true, if this type includes all of the values of the other type
   */
  public abstract boolean subsumes(HiveType other);

  /**
   * Merge the other type into this one. It assumes that subsumes(other) is
   * true.
   * @param other
   */
  public abstract void merge(HiveType other);

  /**
   * Print this type into the stream using a flat structure given the
   * prefix on each element.
   * @param out the stream to print to
   * @param prefix the prefix to add to each field name
   */
  public void printFlat(PrintStream out, String prefix) {
    out.println(prefix + ": " + this);
  }

  public abstract TypeDescription getSchema();
}
