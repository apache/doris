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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.NestedProjection;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * The paimon read type matching the nested column shape Doris asked for.
 *
 * <p>Paimon can push a nested projection down ({@code ReadBuilder.withReadType}), so the shape resolved
 * by {@link NestedProjection} is rebuilt here as paimon's own types and handed to the reader — the rows
 * that come back are already narrow. Paimon's field ids, descriptions and nullability are carried
 * through unchanged: a read type that dropped them would stop matching the files it is meant to prune.
 */
final class PaimonReadTypeProjection {

    private PaimonReadTypeProjection() {
    }

    /**
     * @return the pruned paimon type, or {@code tableType} itself when the whole column was requested —
     *         the caller distinguishes the two to decide between {@code withReadType} and the plain
     *         top-level {@code withProjection}
     */
    static DataType project(DataType tableType, ColumnType requiredType) {
        NestedProjection<DataType> shape =
                NestedProjection.of(requiredType, tableType, PaimonNestedTypeSource.INSTANCE);
        return shape.isIdentity() ? tableType : rebuild(shape);
    }

    private static DataType rebuild(NestedProjection<DataType> shape) {
        switch (shape.getKind()) {
            case STRUCT: {
                RowType rowType = (RowType) shape.getSourceType();
                List<DataField> fields = new ArrayList<DataField>(shape.childCount());
                for (int i = 0; i < shape.childCount(); i++) {
                    DataField field = rowType.getFields().get(shape.sourceChildIndex(i));
                    fields.add(field.newType(rebuild(shape.child(i))));
                }
                return rowType.copy(fields);
            }
            case ARRAY:
                return ((ArrayType) shape.getSourceType())
                        .newElementType(rebuild(shape.elementProjection()));
            case MAP:
                return ((MapType) shape.getSourceType()).newKeyValueType(
                        rebuild(shape.keyProjection()), rebuild(shape.valueProjection()));
            default:
                return shape.getSourceType();
        }
    }
}
