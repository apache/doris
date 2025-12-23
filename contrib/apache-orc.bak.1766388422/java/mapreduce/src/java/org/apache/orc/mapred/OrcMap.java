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

import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * A TreeMap implementation that implements Writable.
 * @param <K> the key type, which must be WritableComparable
 * @param <V> the value type, which must be WritableComparable
 */
public final class OrcMap<K extends WritableComparable,
                          V extends WritableComparable>
    extends TreeMap<K, V> implements WritableComparable<OrcMap<K,V>> {
  private final TypeDescription keySchema;
  private final TypeDescription valueSchema;

  public OrcMap(TypeDescription schema) {
    keySchema = schema.getChildren().get(0);
    valueSchema = schema.getChildren().get(1);
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(size());
    for(Map.Entry<K,V> entry: entrySet()) {
      K key = entry.getKey();
      V value = entry.getValue();
      output.writeByte((key == null ? 0 : 2) | (value == null ? 0 : 1));
      if (key != null) {
        key.write(output);
      }
      if (value != null) {
        value.write(output);
      }
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    clear();
    int size = input.readInt();
    for(int i=0; i < size; ++i) {
      byte flag = input.readByte();
      K key;
      V value;
      if ((flag & 2) != 0) {
        key = (K) OrcStruct.createValue(keySchema);
        key.readFields(input);
      } else {
        key = null;
      }
      if ((flag & 1) != 0) {
        value = (V) OrcStruct.createValue(valueSchema);
        value.readFields(input);
      } else {
        value = null;
      }
      put(key, value);
    }
  }

  @Override
  public int compareTo(OrcMap<K,V> other) {
    if (other == null) {
      return -1;
    }
    int result = keySchema.compareTo(other.keySchema);
    if (result != 0) {
      return result;
    }
    result = valueSchema.compareTo(other.valueSchema);
    if (result != 0) {
      return result;
    }
    Iterator<Map.Entry<K,V>> ourItr = entrySet().iterator();
    Iterator<Map.Entry<K,V>> theirItr = other.entrySet().iterator();
    while (ourItr.hasNext() && theirItr.hasNext()) {
      Map.Entry<K,V> ourItem = ourItr.next();
      Map.Entry<K,V> theirItem = theirItr.next();
      K ourKey = ourItem.getKey();
      K theirKey = theirItem.getKey();
      int val = ourKey.compareTo(theirKey);
      if (val != 0) {
        return val;
      }
      Comparable<V> ourValue = ourItem.getValue();
      V theirValue = theirItem.getValue();
      if (ourValue == null) {
        if (theirValue != null) {
          return 1;
        }
      } else if (theirValue == null) {
        return -1;
      } else {
        val = ourItem.getValue().compareTo(theirItem.getValue());
        if (val != 0) {
          return val;
        }
      }
    }
    if (ourItr.hasNext()) {
      return 1;
    } else if (theirItr.hasNext()) {
      return -1;
    } else {
      return 0;
    }
  }

  @Override
  public boolean equals(Object other) {
    return other != null && other.getClass() == getClass() &&
        compareTo((OrcMap<K,V>) other) == 0;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
