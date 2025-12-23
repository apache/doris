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

package org.apache.orc.bench.spark;

import org.apache.orc.TypeDescription;
import org.apache.spark.sql.types.ArrayType$;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.ByteType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.MapType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType$;

import java.util.ArrayList;
import java.util.List;

public class SparkSchema {

  public static DataType convertToSparkType(TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return BooleanType$.MODULE$;
      case BYTE:
        return ByteType$.MODULE$;
      case SHORT:
        return ShortType$.MODULE$;
      case INT:
        return IntegerType$.MODULE$;
      case LONG:
        return LongType$.MODULE$;
      case FLOAT:
        return FloatType$.MODULE$;
      case DOUBLE:
        return DoubleType$.MODULE$;
      case BINARY:
        return BinaryType$.MODULE$;
      case STRING:
      case CHAR:
      case VARCHAR:
        return StringType$.MODULE$;
      case DATE:
        return DateType$.MODULE$;
      case TIMESTAMP:
        return TimestampType$.MODULE$;
      case DECIMAL:
        return DecimalType$.MODULE$.apply(schema.getPrecision(), schema.getScale());
      case LIST:
        return ArrayType$.MODULE$.apply(
            convertToSparkType(schema.getChildren().get(0)), true);
      case MAP:
        return MapType$.MODULE$.apply(
            convertToSparkType(schema.getChildren().get(0)),
            convertToSparkType(schema.getChildren().get(1)), true);
      case STRUCT: {
        int size = schema.getChildren().size();
        List<StructField> sparkFields = new ArrayList<>(size);
        for(int c=0; c < size; ++c) {
          sparkFields.add(StructField.apply(schema.getFieldNames().get(c),
              convertToSparkType(schema.getChildren().get(c)), true,
              Metadata.empty()));
        }
        return StructType$.MODULE$.apply(sparkFields);
      }
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }
}
