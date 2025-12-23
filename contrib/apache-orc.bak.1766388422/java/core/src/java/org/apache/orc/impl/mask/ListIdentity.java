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
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.orc.DataMask;

/**
 * A data mask for list types that applies the given masks to its
 * children, but doesn't mask at this level.
 */
public class ListIdentity implements DataMask {
  private final DataMask child;

  ListIdentity(DataMask[] child) {
    this.child = child[0];
  }

  @Override
  public void maskData(ColumnVector original, ColumnVector masked, int start, int length) {
    ListColumnVector source = (ListColumnVector) original;
    ListColumnVector target = (ListColumnVector) masked;
    target.noNulls = source.noNulls;
    target.isRepeating = source.isRepeating;
    if (source.isRepeating) {
      if (!source.noNulls && source.isNull[0]) {
        target.isNull[0] = true;
      } else {
        target.lengths[0] = source.lengths[0];
        child.maskData(source.child, target.child, (int) source.offsets[0],
            (int) source.lengths[0]);
      }
    } else if (source.noNulls) {
      for(int r=start; r < start+length; ++r) {
        target.offsets[r] = source.offsets[r];
        target.lengths[r] = source.lengths[r];
        child.maskData(source.child, target.child, (int) target.offsets[r],
            (int) target.lengths[r]);
      }
    } else {
      for(int r=start; r < start+length; ++r) {
        target.isNull[r] = source.isNull[r];
        if (!source.isNull[r]) {
          target.offsets[r] = source.offsets[r];
          target.lengths[r] = source.lengths[r];
          child.maskData(source.child, target.child, (int) target.offsets[r],
              (int) target.lengths[r]);
        }
      }
    }
  }
}
