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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Var.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.plsql.exception.TypeException;
import org.apache.doris.plsql.executor.QueryResult;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * Variable or the result of expression
 */
public class Var {
    // Data types
    public enum Type {
        BOOL, CURSOR, DATE, DECIMAL, DERIVED_TYPE, DERIVED_ROWTYPE, DOUBLE, FILE, IDENT, BIGINT, INTERVAL, ROW,
        RS_LOCATOR, STRING, STRINGLIST, TIMESTAMP, NULL, PL_OBJECT
    }

    public static final String DERIVED_TYPE = "DERIVED%TYPE";
    public static final String DERIVED_ROWTYPE = "DERIVED%ROWTYPE";
    public static Var Empty = new Var();
    public static Var Null = new Var(Type.NULL);

    public String name;
    public Type type;
    public Object value;

    int len;
    int scale;

    boolean constant = false;

    public Var() {
        type = Type.NULL;
    }

    public Var(Var var) {
        name = var.name;
        type = var.type;
        value = var.value;
        len = var.len;
        scale = var.scale;
    }

    public Var(Long value) {
        this.type = Type.BIGINT;
        this.value = value;
    }

    public Var(BigDecimal value) {
        this.type = Type.DECIMAL;
        this.value = value;
    }

    public Var(String name, Long value) {
        this.type = Type.BIGINT;
        this.name = name;
        this.value = value;
    }

    public Var(String value) {
        this.type = Type.STRING;
        this.value = value;
    }

    public Var(Double value) {
        this.type = Type.DOUBLE;
        this.value = value;
    }

    public Var(Date value) {
        this.type = Type.DATE;
        this.value = value;
    }

    public Var(Timestamp value, int scale) {
        this.type = Type.TIMESTAMP;
        this.value = value;
        this.scale = scale;
    }

    public Var(Interval value) {
        this.type = Type.INTERVAL;
        this.value = value;
    }

    public Var(ArrayList<String> value) {
        this.type = Type.STRINGLIST;
        this.value = value;
    }

    public Var(Boolean b) {
        type = Type.BOOL;
        value = b;
    }

    public Var(String name, Row row) {
        this.name = name;
        this.type = Type.ROW;
        this.value = new Row(row);
    }

    public Var(Type type, String name) {
        this.type = type;
        this.name = name;
    }

    public Var(Type type, Object value) {
        this.type = type;
        this.value = value;
    }

    public Var(String name, Type type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public Var(Type type) {
        this.type = type;
    }

    public Var(String name, String type, Integer len, Integer scale, Var def) {
        this.name = name;
        setType(type);
        if (len != null) {
            this.len = len;
        }
        if (scale != null) {
            this.scale = scale;
        }
        if (def != null) {
            cast(def);
        }
    }

    public Var(String name, String type, String len, String scale, Var def) {
        this(name, type, len != null ? Integer.parseInt(len) : null, scale != null ? Integer.parseInt(scale) : null,
                def);
    }

    /**
     * Cast a new value to the variable
     */
    public Var cast(Var val) {
        try {
            if (constant) {
                return this;
            } else if (val.value instanceof Literal) { // At first, ignore type
                value = val.value;
            } else if (val == null || val.value == null) {
                value = null;
            } else if (type == Type.DERIVED_TYPE) {
                type = val.type;
                value = val.value;
            } else if (type == val.type && type == Type.STRING) {
                cast((String) val.value);
            } else if (type == val.type) {
                value = val.value;
            } else if (type == Type.STRING) {
                cast(val.toString());
            } else if (type == Type.BIGINT) {
                if (val.type == Type.STRING) {
                    value = Long.parseLong((String) val.value);
                } else if (val.type == Type.DECIMAL) {
                    value = ((BigDecimal) val.value).longValue();
                }
            } else if (type == Type.DECIMAL) {
                if (val.type == Type.STRING) {
                    value = new BigDecimal((String) val.value);
                } else if (val.type == Type.BIGINT) {
                    value = BigDecimal.valueOf(val.longValue());
                } else if (val.type == Type.DOUBLE) {
                    value = BigDecimal.valueOf(val.doubleValue());
                }
            } else if (type == Type.DOUBLE) {
                if (val.type == Type.STRING) {
                    value = Double.valueOf((String) val.value);
                } else if (val.type == Type.BIGINT || val.type == Type.DECIMAL) {
                    value = Double.valueOf(val.doubleValue());
                }
            } else if (type == Type.DATE) {
                value = org.apache.doris.plsql.Utils.toDate(val.toString());
            } else if (type == Type.TIMESTAMP) {
                value = org.apache.doris.plsql.Utils.toTimestamp(val.toString());
            }
        } catch (NumberFormatException e) {
            throw new TypeException(null, type, val.type, val.value);
        }
        return this;
    }

    public Literal toLiteral() {
        if (value instanceof Literal) {
            return (Literal) value;
        } else {
            return Literal.of(value);
        }
    }

    /**
     * Cast a new string value to the variable
     */
    public Var cast(String val) {
        if (!constant && type == Type.STRING) {
            if (len != 0) {
                int l = val.length();
                if (l > len) {
                    value = val.substring(0, len);
                    return this;
                }
            }
            value = val;
        }
        return this;
    }

    /**
     * Set the new value
     */
    public void setValue(String str) {
        if (!constant && type == Type.STRING) {
            value = str;
        }
    }

    public Var setValue(Long val) {
        if (!constant && type == Type.BIGINT) {
            value = val;
        }
        return this;
    }

    public Var setValue(Boolean val) {
        if (!constant && type == Type.BOOL) {
            value = val;
        }
        return this;
    }

    public void setValue(Object value) {
        if (!constant) {
            this.value = value;
        }
    }

    public Var setValue(QueryResult queryResult, int idx) throws AnalysisException {
        if (queryResult.jdbcType(idx) == Integer.MIN_VALUE) {
            value = queryResult.column(idx);
        } else { // JdbcQueryExecutor
            int type = queryResult.jdbcType(idx);
            if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
                cast(new Var(queryResult.column(idx, String.class)));
            } else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT
                    || type == java.sql.Types.SMALLINT || type == java.sql.Types.TINYINT) {
                cast(new Var(queryResult.column(idx, Long.class)));
            } else if (type == java.sql.Types.DECIMAL || type == java.sql.Types.NUMERIC) {
                cast(new Var(queryResult.column(idx, BigDecimal.class)));
            } else if (type == java.sql.Types.FLOAT || type == java.sql.Types.DOUBLE) {
                cast(new Var(queryResult.column(idx, Double.class)));
            }
        }
        return this;
    }

    public Var setRowValues(QueryResult queryResult) throws AnalysisException {
        Row row = (Row) this.value;
        int idx = 0;
        for (Column column : row.getColumns()) {
            Var var = new Var(column.getName(), column.getType(), (Integer) null, null, null);
            var.setValue(queryResult, idx);
            column.setValue(var);
            idx++;
        }
        return this;
    }

    /**
     * Set the data type from string representation
     */
    public void setType(String type) {
        this.type = defineType(type);
    }

    /**
     * Set the data type from JDBC type code
     */
    void setType(int type) {
        this.type = defineType(type);
    }

    /**
     * Set the variable as constant
     */
    void setConstant(boolean constant) {
        this.constant = constant;
    }

    /**
     * Define the data type from string representation
     * from hive type to plsql var type
     */
    public static Type defineType(String type) {
        if (type == null) {
            return Type.NULL;
        } else if (type.equalsIgnoreCase("INT") || type.equalsIgnoreCase("INTEGER") || type.equalsIgnoreCase("BIGINT")
                || type.equalsIgnoreCase("SMALLINT") || type.equalsIgnoreCase("TINYINT")
                || type.equalsIgnoreCase("BINARY_INTEGER") || type.equalsIgnoreCase("PLS_INTEGER")
                || type.equalsIgnoreCase("SIMPLE_INTEGER") || type.equalsIgnoreCase("INT2")
                || type.equalsIgnoreCase("INT4") || type.equalsIgnoreCase("INT8")) {
            return Type.BIGINT;
        } else if (type.equalsIgnoreCase("CHAR") || type.equalsIgnoreCase("VARCHAR") || type.equalsIgnoreCase(
                "VARCHAR2")
                || type.equalsIgnoreCase("STRING") || type.equalsIgnoreCase("XML")
                || type.equalsIgnoreCase("CHARACTER")) {
            return Type.STRING;
        } else if (type.equalsIgnoreCase("DEC") || type.equalsIgnoreCase("DECIMAL") || type.equalsIgnoreCase("NUMERIC")
                ||
                type.equalsIgnoreCase("NUMBER")) {
            return Type.DECIMAL;
        } else if (type.equalsIgnoreCase("REAL") || type.equalsIgnoreCase("FLOAT") || type.toUpperCase()
                .startsWith("DOUBLE") || type.equalsIgnoreCase("BINARY_FLOAT")
                || type.toUpperCase().startsWith("BINARY_DOUBLE") || type.equalsIgnoreCase("SIMPLE_FLOAT")
                || type.toUpperCase().startsWith("SIMPLE_DOUBLE")) {
            return Type.DOUBLE;
        } else if (type.equalsIgnoreCase("DATE")) {
            return Type.DATE;
        } else if (type.equalsIgnoreCase("TIMESTAMP")) {
            return Type.TIMESTAMP;
        } else if (type.equalsIgnoreCase("BOOL") || type.equalsIgnoreCase("BOOLEAN")) {
            return Type.BOOL;
        } else if (type.equalsIgnoreCase("SYS_REFCURSOR")) {
            return Type.CURSOR;
        } else if (type.equalsIgnoreCase("UTL_FILE.FILE_TYPE")) {
            return Type.FILE;
        } else if (type.toUpperCase().startsWith("RESULT_SET_LOCATOR")) {
            return Type.RS_LOCATOR;
        } else if (type.equalsIgnoreCase(Var.DERIVED_TYPE)) {
            return Type.DERIVED_TYPE;
        } else if (type.equalsIgnoreCase(Type.PL_OBJECT.name())) {
            return Type.PL_OBJECT;
        } else if (type.equalsIgnoreCase(Type.ROW.name())) {
            return Type.ROW;
        }
        return Type.NULL;
    }

    /**
     * Define the data type from JDBC type code
     */
    public static Type defineType(int type) {
        if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
            return Type.STRING;
        } else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT) {
            return Type.BIGINT;
        }
        return Type.NULL;
    }

    /**
     * Remove value
     */
    public void removeValue() {
        type = Type.NULL;
        name = null;
        value = null;
        len = 0;
        scale = 0;
    }

    /**
     * Compare values
     */
    @Override
    public boolean equals(Object obj) {
        if (getClass() != obj.getClass()) {
            return false;
        }
        Var var = (Var) obj;
        if (this == var) {
            return true;
        } else if (var == null || var.value == null || this.value == null) {
            return false;
        }
        if (type == Type.BIGINT) {
            if (var.type == Type.BIGINT && ((Long) value).longValue() == ((Long) var.value).longValue()) {
                return true;
            } else if (var.type == Type.DECIMAL) {
                return equals((BigDecimal) var.value, (Long) value);
            }
        } else if (type == Type.STRING && var.type == Type.STRING && value.equals(var.value)) {
            return true;
        } else if (type == Type.DECIMAL && var.type == Type.DECIMAL
                && ((BigDecimal) value).compareTo((BigDecimal) var.value) == 0) {
            return true;
        } else if (type == Type.DOUBLE) {
            if (var.type == Type.DOUBLE && ((Double) value).compareTo((Double) var.value) == 0) {
                return true;
            } else if (var.type == Type.DECIMAL
                    && ((Double) value).compareTo(((BigDecimal) var.value).doubleValue()) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if variables of different data types are equal
     */
    public boolean equals(BigDecimal d, Long i) {
        return d.compareTo(new BigDecimal(i)) == 0;
    }

    /**
     * Compare values
     */
    public int compareTo(Var v) {
        if (this == v) {
            return 0;
        } else if (v == null) {
            return -1;
        } else if (type == Type.BIGINT && v.type == Type.BIGINT) {
            return ((Long) value).compareTo((Long) v.value);
        } else if (type == Type.DOUBLE && v.type == Type.DECIMAL) {
            return (new BigDecimal((double) value)).compareTo((BigDecimal) v.value);
        } else if (type == Type.STRING && v.type == Type.STRING) {
            return ((String) value).compareTo((String) v.value);
        }
        return -1;
    }

    /**
     * Calculate difference between values in percent
     */
    public BigDecimal percentDiff(Var var) {
        BigDecimal d1 = new Var(Var.Type.DECIMAL).cast(this).decimalValue();
        BigDecimal d2 = new Var(Var.Type.DECIMAL).cast(var).decimalValue();
        if (d1 != null && d2 != null) {
            if (d1.compareTo(BigDecimal.ZERO) != 0) {
                return d1.subtract(d2).abs().multiply(new BigDecimal(100)).divide(d1, 2, RoundingMode.HALF_UP);
            }
        }
        return null;
    }

    /**
     * Increment an integer value
     */
    public Var increment(long i) {
        if (type == Type.BIGINT) {
            value = Long.valueOf(((Long) value).longValue() + i);
        }
        return this;
    }

    /**
     * Decrement an integer value
     */
    public Var decrement(long i) {
        if (type == Type.BIGINT) {
            value = Long.valueOf(((Long) value).longValue() - i);
        }
        return this;
    }

    /**
     * Return an integer value
     */
    public int intValue() {
        if (type == Type.BIGINT) {
            return ((Long) value).intValue();
        } else if (type == Type.STRING) {
            return Integer.parseInt((String) value);
        }
        throw new NumberFormatException();
    }

    /**
     * Return a long integer value
     */
    public long longValue() {
        if (type == Type.BIGINT) {
            return ((Long) value).longValue();
        }
        throw new NumberFormatException();
    }

    /**
     * Return a decimal value
     */
    public BigDecimal decimalValue() {
        if (type == Type.DECIMAL) {
            return (BigDecimal) value;
        }
        throw new NumberFormatException();
    }

    /**
     * Return a double value
     */
    public double doubleValue() {
        if (type == Type.DOUBLE) {
            return ((Double) value).doubleValue();
        } else if (type == Type.BIGINT) {
            return ((Long) value).doubleValue();
        } else if (type == Type.DECIMAL) {
            return ((BigDecimal) value).doubleValue();
        }
        throw new NumberFormatException();
    }

    /**
     * Return true/false for BOOL type
     */
    public boolean isTrue() {
        if (type == Type.BOOL && value != null) {
            return ((Boolean) value).booleanValue();
        }
        return false;
    }

    /**
     * Negate the value
     */
    public void negate() {
        if (value == null) {
            return;
        }
        if (type == Type.BOOL) {
            boolean v = ((Boolean) value).booleanValue();
            value = Boolean.valueOf(!v);
        } else if (type == Type.DECIMAL) {
            BigDecimal v = (BigDecimal) value;
            value = v.negate();
        } else if (type == Type.DOUBLE) {
            Double v = (Double) value;
            value = -v;
        } else if (type == Type.BIGINT) {
            Long v = (Long) value;
            value = -v;
        } else {
            throw new NumberFormatException("invalid type " + type);
        }
    }

    /**
     * Check if the variable contains NULL
     */
    public boolean isNull() {
        return type == Type.NULL || value == null;
    }

    /**
     * Convert value to String
     */
    @Override
    public String toString() {
        if (value instanceof Literal) {
            return value.toString();
        } else if (type == Type.IDENT) {
            return name;
        } else if (value == null) {
            return null;
        } else if (type == Type.BIGINT) {
            return ((Long) value).toString();
        } else if (type == Type.STRING) {
            return (String) value;
        } else if (type == Type.DATE) {
            return ((Date) value).toString();
        } else if (type == Type.TIMESTAMP) {
            int len = 19;
            String t = ((Timestamp) value).toString();   // .0 returned if the fractional part not set
            if (scale > 0) {
                len += scale + 1;
            }
            if (t.length() > len) {
                t = t.substring(0, len);
            }
            return t;
        }
        return value.toString();
    }

    /**
     * Convert value to SQL string - string literals are quoted and escaped, ab'c -&gt; 'ab''c'
     */
    public String toSqlString() {
        if (value == null) {
            return "NULL";
        } else if (type == Type.STRING) {
            return org.apache.doris.plsql.Utils.quoteString((String) value);
        }
        return toString();
    }

    /**
     * Set variable name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get variable name
     */
    public String getName() {
        return name;
    }
}
