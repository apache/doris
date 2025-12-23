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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

/**
 * This defines the input for any filter operation. This is an extension of
 * [[{@link VectorizedRowBatch}]] with schema.
 * <p>
 * This offers a convenience method of finding the column vector from a given column name
 * that the filters can invoke to get access to the column vector.
 */
public class OrcFilterContextImpl implements OrcFilterContext {
  private VectorizedRowBatch batch = null;
  // Cache of field to ColumnVector, this is reset everytime the batch reference changes
  private final Map<String, ColumnVector[]> vectors;
  private final TypeDescription readSchema;
  private final boolean isSchemaCaseAware;

  public OrcFilterContextImpl(TypeDescription readSchema, boolean isSchemaCaseAware) {
    this.readSchema = readSchema;
    this.isSchemaCaseAware = isSchemaCaseAware;
    this.vectors = new HashMap<>();
  }

  public OrcFilterContext setBatch(@NotNull VectorizedRowBatch batch) {
    if (batch != this.batch) {
      this.batch = batch;
      vectors.clear();
    }
    return this;
  }

  /**
   * For testing only
   * @return The batch reference against which the cache is maintained
   */
  VectorizedRowBatch getBatch() {
    return batch;
  }

  @Override
  public void setFilterContext(boolean selectedInUse, int[] selected, int selectedSize) {
    batch.setFilterContext(selectedInUse, selected, selectedSize);
  }

  @Override
  public boolean validateSelected() {
    return batch.validateSelected();
  }

  @Override
  public int[] updateSelected(int i) {
    return batch.updateSelected(i);
  }

  @Override
  public void setSelectedInUse(boolean b) {
    batch.setSelectedInUse(b);
  }

  @Override
  public void setSelected(int[] ints) {
    batch.setSelected(ints);
  }

  @Override
  public void setSelectedSize(int i) {
    batch.setSelectedSize(i);
  }

  @Override
  public void reset() {
    batch.reset();
  }

  @Override
  public boolean isSelectedInUse() {
    return batch.isSelectedInUse();
  }

  @Override
  public int[] getSelected() {
    return batch.getSelected();
  }

  @Override
  public int getSelectedSize() {
    return batch.getSelectedSize();
  }

  // For testing only
  public ColumnVector[] getCols() {
    return batch.cols;
  }

  @Override
  public ColumnVector[] findColumnVector(String name) {
    return vectors.computeIfAbsent(name,
        key -> ParserUtils.findColumnVectors(readSchema,
                                             new ParserUtils.StringPosition(key),
                                             isSchemaCaseAware, batch));
  }
}
