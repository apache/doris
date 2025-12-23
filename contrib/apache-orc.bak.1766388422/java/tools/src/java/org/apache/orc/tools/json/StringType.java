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
 * These are the types that correspond the the JSON string values: string,
 * binary, timestamp, and date.
 */
class StringType extends HiveType {
  StringType(Kind kind) {
    super(kind);
  }

  @Override
  public String toString() {
    switch (kind) {
      case BINARY:
        return "binary";
      case STRING:
        return "string";
      case TIMESTAMP:
        return "timestamp";
      case TIMESTAMP_INSTANT:
        return "timestamp with local time zone";
      case DATE:
        return "date";
      default:
        throw new IllegalArgumentException("Unknown kind " + kind);
    }
  }

  @Override
  public boolean subsumes(HiveType other) {
    return other.getClass() == StringType.class || other.kind == Kind.NULL;
  }

  @Override
  public void merge(HiveType other) {
    // the general case is that everything is a string.
    if (other.getClass() == StringType.class && kind != other.kind) {
      kind = Kind.STRING;
    }
  }

  @Override
  public TypeDescription getSchema() {
    switch (kind) {
      case BINARY:
        return TypeDescription.createBinary();
      case STRING:
        return TypeDescription.createString();
      case TIMESTAMP:
        return TypeDescription.createTimestamp();
      case TIMESTAMP_INSTANT:
        return TypeDescription.createTimestampInstant();
      case DATE:
        return TypeDescription.createDate();
      default:
        throw new IllegalArgumentException("Unknown kind " + kind);
    }
  }
}
