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
package org.apache.orc.mapred;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An in-memory representation of a union type.
 */
public final class OrcUnion implements WritableComparable<OrcUnion> {
  private byte tag;
  private WritableComparable object;
  private final TypeDescription schema;

  public OrcUnion(TypeDescription schema) {
    this.schema = schema;
  }

  public void set(int tag, WritableComparable object) {
    this.tag = (byte) tag;
    this.object = object;
  }

  public byte getTag() {
    return tag;
  }

  public Writable getObject() {
    return object;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != OrcUnion.class) {
      return false;
    }
    OrcUnion oth = (OrcUnion) other;
    if (tag != oth.tag) {
      return false;
    } else if (object == null) {
      return oth.object == null;
    } else {
      return object.equals(oth.object);
    }
  }

  @Override
  public int hashCode() {
    int result = tag;
    if (object != null) {
      result ^= object.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    return "uniontype(" + Integer.toString(tag & 0xff) + ", " + object + ")";
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeByte(tag);
    output.writeBoolean(object != null);
    if (object != null) {
      object.write(output);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    byte oldTag = tag;
    tag = input.readByte();
    if (input.readBoolean()) {
      if (oldTag != tag || object == null) {
        object = OrcStruct.createValue(schema.getChildren().get(tag));
      }
      object.readFields(input);
    } else {
      object = null;
    }
  }

  @Override
  public int compareTo(OrcUnion other) {
    if (other == null) {
      return -1;
    }
    int result = schema.compareTo(other.schema);
    if (result != 0) {
      return result;
    }
    if (tag != other.tag) {
      return tag - other.tag;
    }
    if (object == null) {
      return other.object == null ? 0 : 1;
    }
    return object.compareTo(other.object);
  }
}
