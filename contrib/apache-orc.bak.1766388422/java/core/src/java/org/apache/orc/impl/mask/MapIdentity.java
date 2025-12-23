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
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.orc.DataMask;

/**
 * A data mask for map types that applies the given masks to its
 * children, but doesn't mask at this level.
 */
public class MapIdentity implements DataMask {
  private final DataMask keyMask;
  private final DataMask valueMask;

  MapIdentity(DataMask[] children) {
    this.keyMask = children[0];
    this.valueMask = children[1];
  }

  @Override
  public void maskData(ColumnVector original, ColumnVector masked, int start,
                       int length) {
    MapColumnVector source = (MapColumnVector) original;
    MapColumnVector target = (MapColumnVector) masked;
    target.isRepeating = source.isRepeating;
    target.noNulls = source.noNulls;
    if (source.isRepeating) {
      target.isNull[0] = source.isNull[0];
      if (source.noNulls || !source.isNull[0]) {
        target.lengths[0] = source.lengths[0];
        keyMask.maskData(source.keys, target.keys, (int) source.offsets[0],
            (int) source.lengths[0]);
        valueMask.maskData(source.values, target.values, (int) source.offsets[0],
            (int) source.lengths[0]);      }
    } else if (source.noNulls) {
      for(int r=start; r < start+length; ++r) {
        target.offsets[r] = source.offsets[r];
        target.lengths[r] = source.lengths[r];
        keyMask.maskData(source.keys, target.keys, (int) target.offsets[r],
            (int) target.lengths[r]);
        valueMask.maskData(source.values, target.values, (int) target.offsets[r],
            (int) target.lengths[r]);
      }
    } else {
      for(int r=start; r < start+length; ++r) {
        target.isNull[r] = source.isNull[r];
        if (!source.isNull[r]) {
          target.offsets[r] = source.offsets[r];
          target.lengths[r] = source.lengths[r];
          keyMask.maskData(source.keys, target.keys, (int) target.offsets[r],
              (int) target.lengths[r]);
          valueMask.maskData(source.values, target.values, (int) target.offsets[r],
              (int) target.lengths[r]);
        }
      }
    }
  }
}
