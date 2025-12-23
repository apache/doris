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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.orc.TypeDescription;

import java.util.List;

public class TypeUtils {
  private TypeUtils() {}

  public static ColumnVector createColumn(TypeDescription schema,
                                          TypeDescription.RowBatchVersion version,
                                          int maxSize) {
    switch (schema.getCategory()) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongColumnVector(maxSize);
      case DATE:
        return new DateColumnVector(maxSize);
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampColumnVector(maxSize);
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnVector(maxSize);
      case DECIMAL: {
        int precision = schema.getPrecision();
        int scale = schema.getScale();
        if (version == TypeDescription.RowBatchVersion.ORIGINAL ||
                precision > TypeDescription.MAX_DECIMAL64_PRECISION) {
          return new DecimalColumnVector(maxSize, precision, scale);
        } else {
          return new Decimal64ColumnVector(maxSize, precision, scale);
        }
      }
      case STRING:
      case BINARY:
      case CHAR:
      case VARCHAR:
        return new BytesColumnVector(maxSize);
      case STRUCT: {
        List<TypeDescription> children = schema.getChildren();
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = createColumn(children.get(i), version, maxSize);
        }
        return new StructColumnVector(maxSize,
            fieldVector);
      }
      case UNION: {
        List<TypeDescription> children = schema.getChildren();
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = createColumn(children.get(i), version, maxSize);
        }
        return new UnionColumnVector(maxSize,
            fieldVector);
      }
      case LIST: {
        List<TypeDescription> children = schema.getChildren();
        return new ListColumnVector(maxSize,
            createColumn(children.get(0), version, maxSize));
      }
      case MAP: {
        List<TypeDescription> children = schema.getChildren();
        return new MapColumnVector(maxSize,
            createColumn(children.get(0), version, maxSize),
            createColumn(children.get(1), version, maxSize));
      }
      default:
        throw new IllegalArgumentException("Unknown type " + schema.getCategory());
    }
  }

}
