// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Utils to visit doris and iceberg type
 * @param <T>
 */
public class DorisTypeVisitor<T> {
    public static <T> T visit(Type type, DorisTypeVisitor<T> visitor) {
        if (type instanceof StructType) {
            List<StructField> fields = ((StructType) type).getFields();
            List<T> fieldResults = Lists.newArrayListWithExpectedSize(fields.size());

            for (StructField field : fields) {
                fieldResults.add(visitor.field(
                        field,
                        visit(field.getType(), visitor)));
            }

            return visitor.struct((StructType) type, fieldResults);
        } else if (type instanceof MapType) {
            return visitor.map((MapType) type,
                    visit(((MapType) type).getKeyType(), visitor),
                    visit(((MapType) type).getValueType(), visitor));
        } else if (type instanceof ArrayType) {
            return visitor.array(
                    (ArrayType) type,
                    visit(((ArrayType) type).getItemType(), visitor));
        } else {
            return visitor.atomic(type);
        }
    }

    public T struct(StructType struct, List<T> fieldResults) {
        return null;
    }

    public T field(StructField field, T typeResult) {
        return null;
    }

    public T array(ArrayType array, T elementResult) {
        return null;
    }

    public T map(MapType map, T keyResult, T valueResult) {
        return null;
    }

    public T atomic(Type atomic) {
        return null;
    }
}
