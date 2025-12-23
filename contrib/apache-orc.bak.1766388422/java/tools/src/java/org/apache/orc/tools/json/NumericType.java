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

/**
 * A type that represents all of the numeric types: byte, short, int, long,
 * float, double, and decimal.
 */
class NumericType extends HiveType {
  // the maximum number of digits before the decimal
  int intDigits;
  // the maximum number of digits after the decimal
  int scale;

  NumericType(Kind kind, int intDigits, int scale) {
    super(kind);
    this.intDigits = intDigits;
    this.scale = scale;
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other)) {
      NumericType otherNumber = (NumericType) other;
      return intDigits == otherNumber.intDigits || scale == otherNumber.scale;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 41 + (intDigits * 17) + scale;
  }

  @Override
  public String toString() {
    switch (kind) {
      case BYTE:
        return "tinyint";
      case SHORT:
        return "smallint";
      case INT:
        return "int";
      case LONG:
        return "bigint";
      case DECIMAL:
        return "decimal(" + (intDigits + scale) + "," + scale + ")";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      default:
        throw new IllegalArgumentException("Unknown kind " +  kind);
    }
  }

  @Override
  public boolean subsumes(HiveType other) {
    return other.getClass() == NumericType.class || other.kind == Kind.NULL;
  }

  @Override
  public void merge(HiveType other) {
    if (other.getClass() == NumericType.class) {
      NumericType otherNumber = (NumericType) other;
      this.intDigits = Math.max(this.intDigits, otherNumber.intDigits);
      this.scale = Math.max(this.scale, otherNumber.scale);
      if (kind.rank < other.kind.rank) {
        kind = other.kind;
      }
    }
  }

  @Override
  public TypeDescription getSchema() {
    switch (kind) {
      case BYTE:
        return TypeDescription.createByte();
      case SHORT:
        return TypeDescription.createShort();
      case INT:
        return TypeDescription.createInt();
      case LONG:
        return TypeDescription.createLong();
      case DECIMAL:
        return TypeDescription.createDecimal()
            .withScale(scale).withPrecision(intDigits+scale);
      case FLOAT:
        return TypeDescription.createFloat();
      case DOUBLE:
        return TypeDescription.createDouble();
      default:
        throw new IllegalArgumentException("Unknown kind " +  kind);
    }
  }
}
