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
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.orc.DataMask;

/**
 * A data mask for struct types that applies the given masks to its
 * children, but doesn't mask at this level.
 */
public class StructIdentity implements DataMask {
  private final DataMask[] children;

  StructIdentity(DataMask[] children) {
    this.children = children;
  }

  @Override
  public void maskData(ColumnVector original, ColumnVector masked, int start,
                       int length) {
    StructColumnVector source = (StructColumnVector) original;
    StructColumnVector target = (StructColumnVector) masked;
    target.isRepeating = source.isRepeating;
    target.noNulls = source.noNulls;
    if (source.isRepeating) {
      target.isNull[0] = source.isNull[0];
      if (source.noNulls || !source.isNull[0]) {
        for (int c = 0; c < children.length; ++c) {
          children[c].maskData(source.fields[c], target.fields[c], 0, 1);
        }
      }
    } else if (source.noNulls) {
      for (int c = 0; c < children.length; ++c) {
        children[c].maskData(source.fields[c], target.fields[c], start, length);
      }
    } else {
      // process the children in runs of non-null values
      int batchStart = start;
      while (batchStart < start + length) {
        int r = batchStart;
        while (r < start + length && !source.isNull[r]) {
          r += 1;
        }
        if (r != batchStart) {
          for(int c=0; c < children.length; ++c) {
            children[c].maskData(source.fields[c], target.fields[c],
                batchStart, r - batchStart);
          }
        }
        batchStart = r;
        while (batchStart < start + length && source.isNull[batchStart]) {
          batchStart += 1;
        }
      }
    }
  }
}
