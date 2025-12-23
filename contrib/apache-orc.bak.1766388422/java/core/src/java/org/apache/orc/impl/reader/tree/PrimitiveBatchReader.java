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
package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;

public class PrimitiveBatchReader extends BatchReader {

  public PrimitiveBatchReader(TypeReader rowReader) {
    super(rowReader);
  }

  @Override
  public void nextBatch(VectorizedRowBatch batch,
                        int batchSize,
                        TypeReader.ReadPhase readPhase) throws IOException {
    batch.cols[0].reset();
    batch.cols[0].ensureSize(batchSize, false);
    rootType.nextVector(batch.cols[0], null, batchSize, batch, readPhase);
    resetBatch(batch, batchSize);
  }

  public void startStripe(StripePlanner planner, TypeReader.ReadPhase readPhase)
      throws IOException {
    rootType.startStripe(planner, readPhase);
  }

  public void skipRows(long rows, TypeReader.ReadPhase readPhase) throws IOException {
    rootType.skipRows(rows, readPhase);
  }

  public void seek(PositionProvider[] index, TypeReader.ReadPhase readPhase) throws IOException {
    rootType.seek(index, readPhase);
  }
}
