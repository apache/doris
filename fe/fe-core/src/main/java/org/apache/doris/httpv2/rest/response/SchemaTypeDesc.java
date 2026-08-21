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

package org.apache.doris.httpv2.rest.response;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Recursively structured type description for table schema HTTP APIs.
 *
 * <p>Only attributes that apply to a type are serialized. Complex types point to child
 * {@code SchemaTypeDesc} instances, allowing clients to traverse arbitrarily nested ARRAY, MAP,
 * and STRUCT definitions. JSON property names use snake case to match the surrounding schema API.</p>
 */
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class SchemaTypeDesc {
    // Doris primitive type name. Decimal V3 storage widths remain distinguishable, for example
    // DECIMAL32 and DECIMAL64.
    private final String kind;

    // Complete SQL representation intended for display and compatibility fallback. Clients should
    // prefer the structured fields below when interpreting a type.
    private final String sql;

    // Scalar attributes. Null means that the attribute does not apply to this type.
    private Integer precision;
    private Integer scale;
    private Integer length;

    // ARRAY attributes. containsNull describes element nullability, and element is the recursively
    // described item type.
    private Boolean containsNull;
    private SchemaTypeDesc element;

    // MAP attributes. Nullability and type metadata are reported independently for keys and values.
    private Boolean keyContainsNull;
    private Boolean valueContainsNull;
    private SchemaTypeDesc key;
    private SchemaTypeDesc value;

    // STRUCT attributes in declaration order.
    private List<StructFieldDesc> fields;

    private SchemaTypeDesc(Type type) {
        this.kind = type.getPrimitiveType().toString();
        this.sql = type.toSql();
    }

    /**
     * Converts a Doris catalog type into its public recursive schema representation.
     *
     * <p>The primitive name and SQL form are present on every node. Type-specific metadata is added
     * only on nodes where it is meaningful, so Jackson can omit unrelated fields from the response.</p>
     *
     * @param type catalog type to expose through the schema API
     * @return recursively structured description of {@code type}
     */
    public static SchemaTypeDesc fromType(Type type) {
        SchemaTypeDesc result = new SchemaTypeDesc(type);
        PrimitiveType primitiveType = type.getPrimitiveType();
        switch (primitiveType) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                result.containsNull = arrayType.getContainsNull();
                result.element = fromType(arrayType.getItemType());
                break;
            case MAP:
                MapType mapType = (MapType) type;
                result.keyContainsNull = mapType.getIsKeyContainsNull();
                result.valueContainsNull = mapType.getIsValueContainsNull();
                result.key = fromType(mapType.getKeyType());
                result.value = fromType(mapType.getValueType());
                break;
            case STRUCT:
                StructType structType = (StructType) type;
                result.fields = structType.getFields().stream()
                        .map(StructFieldDesc::fromField)
                        .collect(Collectors.toList());
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                ScalarType decimalType = (ScalarType) type;
                result.precision = decimalType.getPrecision();
                result.scale = decimalType.getScalarScale();
                break;
            case CHAR:
            case VARCHAR:
                result.length = ((ScalarType) type).getLength();
                break;
            case DATETIMEV2:
            case TIMEV2:
            case TIMESTAMPTZ:
                result.scale = ((ScalarType) type).getScalarScale();
                break;
            default:
                break;
        }
        return result;
    }

    /**
     * One named child of a STRUCT type.
     *
     * <p>Field order is preserved by the containing list. {@code containsNull} describes the
     * nested field itself, while {@code type} recursively describes its value type.</p>
     */
    @Getter
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class StructFieldDesc {
        private final String name;
        private final boolean containsNull;
        private final SchemaTypeDesc type;

        private StructFieldDesc(StructField field) {
            this.name = field.getName();
            this.containsNull = field.getContainsNull();
            this.type = SchemaTypeDesc.fromType(field.getType());
        }

        private static StructFieldDesc fromField(StructField field) {
            return new StructFieldDesc(field);
        }
    }
}
