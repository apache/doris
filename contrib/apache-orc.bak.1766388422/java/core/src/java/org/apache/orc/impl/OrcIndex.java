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

package org.apache.orc.impl;

import org.apache.orc.OrcProto;

public final class OrcIndex {
  OrcProto.RowIndex[] rowGroupIndex;
  OrcProto.Stream.Kind[] bloomFilterKinds;
  OrcProto.BloomFilterIndex[] bloomFilterIndex;

  public OrcIndex(OrcProto.RowIndex[] rgIndex,
                  OrcProto.Stream.Kind[] bloomFilterKinds,
                  OrcProto.BloomFilterIndex[] bfIndex) {
    this.rowGroupIndex = rgIndex;
    this.bloomFilterKinds = bloomFilterKinds;
    this.bloomFilterIndex = bfIndex;
  }

  public OrcProto.RowIndex[] getRowGroupIndex() {
    return rowGroupIndex;
  }

  public OrcProto.BloomFilterIndex[] getBloomFilterIndex() {
    return bloomFilterIndex;
  }

  public OrcProto.Stream.Kind[] getBloomFilterKinds() {
    return bloomFilterKinds;
  }

  public void setRowGroupIndex(OrcProto.RowIndex[] rowGroupIndex) {
    this.rowGroupIndex = rowGroupIndex;
  }
}
