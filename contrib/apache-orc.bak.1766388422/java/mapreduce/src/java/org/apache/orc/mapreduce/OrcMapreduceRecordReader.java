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
package org.apache.orc.mapreduce;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.List;

/**
 * This record reader implements the org.apache.hadoop.mapreduce API.
 * It is in the org.apache.orc.mapred package to share implementation with
 * the mapred API record reader.
 * @param <V> the root type of the file
 */
public class OrcMapreduceRecordReader<V extends WritableComparable>
    extends org.apache.hadoop.mapreduce.RecordReader<NullWritable, V> {
  private final TypeDescription schema;
  private final RecordReader batchReader;
  private final VectorizedRowBatch batch;
  private int rowInBatch;
  private final V row;

  public OrcMapreduceRecordReader(RecordReader reader,
                                  TypeDescription schema) throws IOException {
    this.batchReader = reader;
    this.batch = schema.createRowBatch();
    this.schema = schema;
    rowInBatch = 0;
    this.row = (V) OrcStruct.createValue(schema);
  }

  public OrcMapreduceRecordReader(Reader fileReader,
                                  Reader.Options options) throws IOException {
    this(fileReader, options, options.getRowBatchSize());
  }

  public OrcMapreduceRecordReader(Reader fileReader,
                                  Reader.Options options,
                                  int rowBatchSize) throws IOException {
    this.batchReader = fileReader.rows(options);
    if (options.getSchema() == null) {
      schema = fileReader.getSchema();
    } else {
      schema = options.getSchema();
    }
    this.batch = schema.createRowBatch(rowBatchSize);
    rowInBatch = 0;
    this.row = (V) OrcStruct.createValue(schema);
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   * @throws IOException
   */
  boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      rowInBatch = 0;
      return batchReader.nextBatch(batch);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    batchReader.close();
  }

  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) {
    // nothing required
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!ensureBatch()) {
      return false;
    }
    int rowIdx = batch.selectedInUse ? batch.selected[rowInBatch] : rowInBatch;
    if (schema.getCategory() == TypeDescription.Category.STRUCT) {
      OrcStruct result = (OrcStruct) row;
      List<TypeDescription> children = schema.getChildren();
      int numberOfChildren = children.size();
      for(int i=0; i < numberOfChildren; ++i) {
        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], rowIdx,
            children.get(i), result.getFieldValue(i)));
      }
    } else {
      OrcMapredRecordReader.nextValue(batch.cols[0], rowIdx, schema, row);
    }
    rowInBatch += 1;
    return true;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public V getCurrentValue() throws IOException, InterruptedException {
    return row;
  }

  @Override
  public float getProgress() throws IOException {
    return batchReader.getProgress();
  }
}
