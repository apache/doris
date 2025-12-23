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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.TypeDescription;

import java.util.List;

/**
 * HiveUtilities that need the non-public methods from Hive.
 */
public class OrcBenchmarkUtilities {

  public static StructObjectInspector createObjectInspector(TypeDescription schema) {
    List<OrcProto.Type> types = OrcUtils.getOrcTypes(schema);
    return (StructObjectInspector) OrcStruct.createObjectInspector(0, types);
  }

  public static Writable nextObject(VectorizedRowBatch batch,
                                    TypeDescription schema,
                                    int rowId,
                                    Writable obj) {
    OrcStruct result = (OrcStruct) obj;
    if (result == null) {
      result = new OrcStruct(batch.cols.length);
    }
    List<TypeDescription> childrenTypes = schema.getChildren();
    for(int c=0; c < batch.cols.length; ++c) {
      result.setFieldValue(c, RecordReaderImpl.nextValue(batch.cols[c], rowId,
          childrenTypes.get(c), result.getFieldValue(c)));
    }
    return result;
  }
}
