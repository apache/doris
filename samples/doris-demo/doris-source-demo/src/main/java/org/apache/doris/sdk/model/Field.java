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

package org.apache.doris.sdk.model;

import java.util.Objects;

public class Field {
    private final String name;
    private final String type;
    private final String comment;
    private final int precision;
    private final int scale;
    private final String aggregation_type;

    public Field(String name, String type, String comment, int precision, int scale, String aggregation_type) {
        this.name = name;
        this.type = type;
        this.comment = comment;
        this.precision = precision;
        this.scale = scale;
        this.aggregation_type = aggregation_type;
    }

    public String getAggregation_type() {
        return aggregation_type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment, precision, scale);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field field = (Field) o;
        return precision == field.precision && scale == field.scale && Objects.equals(name, field.name)
                && Objects.equals(type, field.type) && Objects.equals(comment, field.comment);
    }

    @Override
    public String toString() {
        return "Field{" + "name='" + name + '\'' + ", type='" + type + '\'' + ", comment='" + comment + '\''
                + ", precision=" + precision + ", scale=" + scale + '}';
    }
}
