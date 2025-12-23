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

import org.apache.hadoop.hive.ql.exec.vector.MultiValuedColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcMapredRecordWriter;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OrcMapreduceRecordWriter<V extends Writable>
    extends RecordWriter<NullWritable, V> {

  private final Writer writer;
  private final VectorizedRowBatch batch;
  private final TypeDescription schema;
  private final boolean isTopStruct;
  private final List<MultiValuedColumnVector> variableLengthColumns =
      new ArrayList<>();
  private final int maxChildLength;

  public OrcMapreduceRecordWriter(Writer writer) {
    this(writer, VectorizedRowBatch.DEFAULT_SIZE);
  }

  public OrcMapreduceRecordWriter(Writer writer,
                                  int rowBatchSize) {
    this(writer, rowBatchSize,
        (Integer) OrcConf.ROW_BATCH_CHILD_LIMIT.getDefaultValue());
  }

  public OrcMapreduceRecordWriter(Writer writer,
                                  int rowBatchSize,
                                  int maxChildLength) {
    this.writer = writer;
    schema = writer.getSchema();
    this.batch = schema.createRowBatch(rowBatchSize);
    isTopStruct = schema.getCategory() == TypeDescription.Category.STRUCT;
    OrcMapredRecordWriter.addVariableLengthColumns(variableLengthColumns, batch);
    this.maxChildLength = maxChildLength;
  }

  @Override
  public void write(NullWritable nullWritable, V v) throws IOException {
    // if the batch is full, write it out.
    if (batch.size == batch.getMaxSize() ||
        OrcMapredRecordWriter.getMaxChildLength(variableLengthColumns) >= maxChildLength) {
      writer.addRowBatch(batch);
      batch.reset();
    }

    // add the new row
    int row = batch.size++;
    // skip over the OrcKey or OrcValue
    if (v instanceof OrcKey) {
      v = (V)((OrcKey) v).key;
    } else if (v instanceof OrcValue) {
      v = (V)((OrcValue) v).value;
    }
    if (isTopStruct) {
      for(int f=0; f < schema.getChildren().size(); ++f) {
        OrcMapredRecordWriter.setColumn(schema.getChildren().get(f),
            batch.cols[f], row, ((OrcStruct) v).getFieldValue(f));
      }
    } else {
      OrcMapredRecordWriter.setColumn(schema, batch.cols[0], row, v);
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
  }
}
