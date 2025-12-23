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
package org.apache.orc.impl.mask;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.orc.DataMask;

/**
 * A data mask for union types that applies the given masks to its
 * children, but doesn't mask at this level.
 */
public class UnionIdentity implements DataMask {
  private final DataMask[] children;

  UnionIdentity(DataMask[] children) {
    this.children = children;
  }

  @Override
  public void maskData(ColumnVector original, ColumnVector masked, int start,
                       int length) {
    UnionColumnVector source = (UnionColumnVector) original;
    UnionColumnVector target = (UnionColumnVector) masked;
    target.isRepeating = source.isRepeating;
    target.noNulls = source.noNulls;
    if (source.isRepeating) {
      target.isNull[0] = source.isNull[0];
      if (source.noNulls || !source.isNull[0]) {
        int tag = source.tags[0];
        target.tags[0] = tag;
        children[tag].maskData(source.fields[tag], target.fields[tag], 0, 1);
      }
    } else if (source.noNulls) {
      for (int r = start; r < start + length; ++r) {
        int tag = source.tags[r];
        target.tags[r] = tag;
        children[tag].maskData(source.fields[tag], target.fields[tag], r, 1);
      }
    } else {
      for(int r= start; r < start + length; ++r) {
        target.isNull[r] = source.isNull[r];
        if (!source.isNull[r]) {
          int tag = source.tags[r];
          target.tags[r] = tag;
          children[tag].maskData(source.fields[tag], target.fields[tag], r, 1);
        }
      }
    }
  }
}
