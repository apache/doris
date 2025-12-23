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

package org.apache.orc.mapred;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MultiValuedColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IntWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

public class TestOrcRecordWriter {

  /**
   * Test finding the multi-value columns.
   */
  @Test
  public void testFindingMultiValueColumns()  {
    // Make sure that we find all the multi-value columns from a batch.
    TypeDescription schema = TypeDescription.fromString("struct<x:struct<" +
        "x:uniontype<int,array<array<int>>,map<array<int>,array<int>>>>>");
    VectorizedRowBatch batch = schema.createRowBatchV2();
    List<MultiValuedColumnVector> result = new ArrayList<>();
    OrcMapredRecordWriter.addVariableLengthColumns(result, batch);
    assertEquals(5, result.size());
    assertEquals(ColumnVector.Type.LIST, result.get(0).type);
    assertEquals(ColumnVector.Type.LIST, result.get(1).type);
    assertEquals(ColumnVector.Type.MAP, result.get(2).type);
    assertEquals(ColumnVector.Type.LIST, result.get(3).type);
    assertEquals(ColumnVector.Type.LIST, result.get(4).type);
  }

  /**
   * Test the child element limit flushes the writer.
   */
  @Test
  public void testChildElementLimit() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:array<int>>");
    Writer mockWriter = Mockito.mock(Writer.class);
    Mockito.when(mockWriter.getSchema()).thenReturn(schema);
    OrcMapredRecordWriter<OrcStruct> recordWriter =
        new OrcMapredRecordWriter<>(mockWriter, 1024, 10);
    OrcStruct record = new OrcStruct(schema);
    OrcList list = new OrcList(schema.getChildren().get(0));
    record.setFieldValue(0, list);
    list.add(new IntWritable(1));
    list.add(new IntWritable(2));
    Mockito.verify(mockWriter, times(0)).addRowBatch(any());
    for(int i=0; i < 11; i++) {
      recordWriter.write(null, record);
    }
    // We've written 11 rows with 2 integers each, so we should have written
    // 2 batches of 5 rows.
    Mockito.verify(mockWriter, times(2)).addRowBatch(any());
  }
}
