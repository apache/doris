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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;

/**
 * Handles the Struct rootType for batch handling. The handling assumes that the root
 * {@link org.apache.orc.impl.TreeReaderFactory.StructTreeReader} no nulls. Root Struct vector is
 * not represented as part of the final {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 */
public class StructBatchReader extends BatchReader {
  // The reader context including row-filtering details
  private final TreeReaderFactory.Context context;
  private final OrcFilterContextImpl filterContext;
  private final TreeReaderFactory.StructTreeReader structReader;

  public StructBatchReader(TypeReader rowReader, TreeReaderFactory.Context context) {
    super(rowReader);
    this.context = context;
    this.filterContext = new OrcFilterContextImpl(context.getSchemaEvolution().getReaderSchema(),
                                                  context.getSchemaEvolution()
                                                    .isSchemaEvolutionCaseAware());
    structReader = (TreeReaderFactory.StructTreeReader) rowReader;
  }

  private void readBatchColumn(VectorizedRowBatch batch,
                               TypeReader child,
                               int batchSize,
                               int index,
                               TypeReader.ReadPhase readPhase)
    throws IOException {
    ColumnVector colVector = batch.cols[index];
    if (colVector != null) {
      if (readPhase.contains(child.getReaderCategory())) {
        // Reset the column vector only if the current column is being processed. If the children
        // are being processed then we should reset the parent e.g. PARENT_FILTER during FOLLOWERS
        // read phase.
        colVector.reset();
        colVector.ensureSize(batchSize, false);
      }
      child.nextVector(colVector, null, batchSize, batch, readPhase);
    }
  }

  @Override
  public void nextBatch(VectorizedRowBatch batch, int batchSize, TypeReader.ReadPhase readPhase)
    throws IOException {
    if (readPhase == TypeReader.ReadPhase.ALL || readPhase == TypeReader.ReadPhase.LEADERS) {
      // selectedInUse = true indicates that the selected vector should be used to determine
      // valid rows in the batch
      batch.selectedInUse = false;
    }
    nextBatchForLevel(batch, batchSize, readPhase);

    if (readPhase == TypeReader.ReadPhase.ALL || readPhase == TypeReader.ReadPhase.LEADERS) {
      // Set the batch size when reading everything or when reading FILTER columns
      batch.size = batchSize;
    }

    if (readPhase == TypeReader.ReadPhase.LEADERS) {
      // Apply filter callback to reduce number of # rows selected for decoding in the next
      // TreeReaders
      if (this.context.getColumnFilterCallback() != null) {
        this.context.getColumnFilterCallback().accept(filterContext.setBatch(batch));
      }
    }
  }

  private void nextBatchForLevel(
      VectorizedRowBatch batch, int batchSize, TypeReader.ReadPhase readPhase)
      throws IOException {
    TypeReader[] children = structReader.fields;
    for (int i = 0; i < children.length && (vectorColumnCount == -1 || i < vectorColumnCount);
        ++i) {
      if (TypeReader.shouldProcessChild(children[i], readPhase)) {
        readBatchColumn(batch, children[i], batchSize, i, readPhase);
      }
    }
  }

  @Override
  public void startStripe(StripePlanner planner,  TypeReader.ReadPhase readPhase)
      throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (TypeReader.shouldProcessChild(children[i], readPhase)) {
        children[i].startStripe(planner, readPhase);
      }
    }
  }

  @Override
  public void skipRows(long rows,  TypeReader.ReadPhase readerCategory) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (TypeReader.shouldProcessChild(children[i], readerCategory)) {
        children[i].skipRows(rows, readerCategory);
      }
    }
  }

  @Override
  public void seek(PositionProvider[] index, TypeReader.ReadPhase readPhase) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (TypeReader.shouldProcessChild(children[i], readPhase)) {
        children[i].seek(index, readPhase);
      }
    }
  }
}
