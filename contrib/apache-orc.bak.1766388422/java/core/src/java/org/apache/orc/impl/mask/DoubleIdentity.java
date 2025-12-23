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
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.DataMask;

/**
 * An identity data mask for floating point types.
 */
public class DoubleIdentity implements DataMask {
  @Override
  public void maskData(ColumnVector original, ColumnVector masked, int start,
                       int length) {
    DoubleColumnVector target = (DoubleColumnVector) masked;
    DoubleColumnVector source = (DoubleColumnVector) original;
    target.isRepeating = source.isRepeating;
    target.noNulls = source.noNulls;
    if (source.isRepeating) {
      target.vector[0] = source.vector[0];
      target.isNull[0] = source.isNull[0];
    } else if (source.noNulls) {
      for(int r = start; r < start + length; ++r) {
        target.vector[r] = source.vector[r];
      }
    } else {
      for(int r = start; r < start + length; ++r) {
        target.isNull[r] = source.isNull[r];
        target.vector[r] = source.vector[r];
      }
    }
  }
}
