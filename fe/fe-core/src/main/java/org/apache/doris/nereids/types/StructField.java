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

package org.apache.doris.nereids.types;

import org.apache.doris.nereids.util.Utils;

import java.util.Objects;

/**
 * A field inside a StructType.
 */
public class StructField {

    private final String name;
    private final DataType dataType;
    private final boolean nullable;
    private final String comment;

    /**
     * StructField Constructor
     *  @param name The name of this field
     *  @param dataType The data type of this field
     *  @param nullable Indicates if values of this field can be `null` values
     */
    public StructField(String name, DataType dataType, boolean nullable, String comment) {
        this.name = Objects.requireNonNull(name, "name should not be null");
        this.dataType = Objects.requireNonNull(dataType, "dataType should not be null");
        this.nullable = nullable;
        this.comment = Objects.requireNonNull(comment, "comment should not be null");
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getComment() {
        return comment;
    }

    public StructField conversion() {
        if (this.dataType.equals(dataType.conversion())) {
            return this;
        }
        return withDataType(dataType.conversion());
    }

    public StructField withDataType(DataType dataType) {
        return new StructField(name, dataType, nullable, comment);
    }

    public StructField withDataTypeAndNullable(DataType dataType, boolean nullable) {
        return new StructField(name, dataType, nullable, comment);
    }

    public org.apache.doris.catalog.StructField toCatalogDataType() {
        return new org.apache.doris.catalog.StructField(
                name, dataType.toCatalogDataType(), comment, nullable);
    }

    public String toSql() {
        return name + ":" + dataType.toSql()
                + (nullable ? " " : " NOT NULL")
                + (comment.isEmpty() ? "" : " COMMENT " + comment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StructField that = (StructField) o;
        return nullable == that.nullable && Objects.equals(name, that.name) && Objects.equals(dataType,
                that.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, nullable);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("StructField",
                "name", name,
                "dataType", dataType,
                "nullable", nullable,
                "comment", comment);
    }
}
