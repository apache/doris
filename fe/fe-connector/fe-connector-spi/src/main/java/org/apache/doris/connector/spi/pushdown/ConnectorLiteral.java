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

package org.apache.doris.connector.spi.pushdown;

import org.apache.doris.connector.spi.ConnectorType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A literal value expression.
 *
 * <p>The value is a standard Java type. The engine produces exactly these eight shapes — see the package
 * javadoc (Rule 4) for the full Doris-type-to-Java-class table: {@code null}, {@code Boolean}, {@code Long},
 * {@code Double}, {@code BigDecimal}, {@code String}, {@code LocalDate}, {@code LocalDateTime}.</p>
 *
 * <p>Two consequences worth stating outright, because both have produced wrong connector code:
 * <b>{@code Integer} never arrives</b> (every integral type, {@code TINYINT} through {@code BIGINT}, is a
 * {@code Long}; the {@link #ofInt} factory exists for tests), and <b>{@code LARGEINT} arrives as a decimal
 * {@code String}</b>, not as a {@code BigInteger}. A converter that switches on the Java class must have a
 * fall-through for the {@code String} case rather than assuming a numeric column carries a numeric object.</p>
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

    /**
     * Returns the value, which is {@code null} for a NULL literal.
     *
     * <p>A NULL literal is a legal operand of any comparison, so check {@link #isNull()} FIRST. In particular
     * {@code ConnectorComparison.Operator#EQ_FOR_NULL} means {@code IS NULL} only when this is null and plain
     * equality otherwise; conflating the two loses rows (see {@link ConnectorComparison}).</p>
     */
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
