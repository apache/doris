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

package org.apache.doris.connector.api.pushdown;

import org.apache.doris.connector.api.ConnectorType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A literal value expression. The value must be a standard Java type
 * (null, Boolean, Integer, Long, Double, String, BigDecimal, LocalDate, LocalDateTime).
 */
public final class ConnectorLiteral implements ConnectorExpression {

    private static final long serialVersionUID = 1L;

    private final ConnectorType type;
    private final Object value;

    public ConnectorLiteral(ConnectorType type, Object value) {
        this.type = Objects.requireNonNull(type, "type");
        this.value = value;
    }

    public static ConnectorLiteral ofNull(ConnectorType type) {
        return new ConnectorLiteral(type, null);
    }

    public static ConnectorLiteral ofBoolean(boolean val) {
        return new ConnectorLiteral(ConnectorType.of("BOOLEAN"), val);
    }

    public static ConnectorLiteral ofInt(int val) {
        return new ConnectorLiteral(ConnectorType.of("INT"), val);
    }

    public static ConnectorLiteral ofLong(long val) {
        return new ConnectorLiteral(ConnectorType.of("BIGINT"), val);
    }

    public static ConnectorLiteral ofDouble(double val) {
        return new ConnectorLiteral(ConnectorType.of("DOUBLE"), val);
    }

    public static ConnectorLiteral ofString(String val) {
        return new ConnectorLiteral(ConnectorType.of("STRING"), val);
    }

    public static ConnectorLiteral ofDecimal(BigDecimal val, int precision, int scale) {
        return new ConnectorLiteral(ConnectorType.of("DECIMALV3", precision, scale), val);
    }

    public static ConnectorLiteral ofDate(LocalDate val) {
        return new ConnectorLiteral(ConnectorType.of("DATEV2"), val);
    }

    public static ConnectorLiteral ofDatetime(LocalDateTime val) {
        return new ConnectorLiteral(ConnectorType.of("DATETIMEV2"), val);
    }

    public ConnectorType getType() {
        return type;
    }

    /** Returns the value (may be null for NULL literals). */
    public Object getValue() {
        return value;
    }

    public boolean isNull() {
        return value == null;
    }

    @Override
    public List<ConnectorExpression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return value == null ? "NULL" : value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorLiteral)) {
            return false;
        }
        ConnectorLiteral that = (ConnectorLiteral) o;
        return type.equals(that.type) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }
}
