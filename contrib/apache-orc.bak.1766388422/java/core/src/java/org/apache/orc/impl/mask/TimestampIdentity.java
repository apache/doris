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
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.DataMask;

class TimestampIdentity implements DataMask {
  @Override
  public void maskData(ColumnVector original, ColumnVector masked, int start,
                       int length) {
    TimestampColumnVector target = (TimestampColumnVector) masked;
    TimestampColumnVector source = (TimestampColumnVector) original;
    target.noNulls = source.noNulls;
    target.isRepeating = source.isRepeating;
    if (original.isRepeating) {
      target.time[0] = source.time[0];
      target.nanos[0] = source.nanos[0];
      target.isNull[0] = source.isNull[0];
    } else if (source.noNulls) {
      for(int r = start; r < start + length; ++r) {
        target.time[r] = source.time[r];
        target.nanos[r] = source.nanos[r];
      }
    } else {
      for(int r = start; r < start + length; ++r) {
        target.time[r] = source.time[r];
        target.nanos[r] = source.nanos[r];
        target.isNull[r] = source.isNull[r];
      }
    }
  }
}


