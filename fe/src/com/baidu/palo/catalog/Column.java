// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.catalog;

import com.baidu.palo.analysis.DateLiteral;
import com.baidu.palo.analysis.DecimalLiteral;
import com.baidu.palo.analysis.FloatLiteral;
import com.baidu.palo.analysis.IntLiteral;
import com.baidu.palo.analysis.LargeIntLiteral;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.FeNameFormat;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.thrift.TColumn;
import com.baidu.palo.thrift.TColumnType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class represents the column-related metadata.
 */
public class Column implements Writable {
    private static final Logger LOG = LogManager.getLogger(Column.class);
    private static final String HLL_EMPTY_SET = "0";
    private String name;
    private ColumnType columnType;
    private AggregateType aggregationType;
    private boolean isAggregationTypeImplicit;
    private boolean isKey;
    private boolean isAllowNull;
    private String defaultValue;
    private String comment;
    
    private ColumnStats stats;     // cardinality and selectivity etc.
    
    public Column() {
        this.name = "";
        this.columnType = new ColumnType();
        this.isAggregationTypeImplicit = false;
        this.isKey = false;
        this.stats = new ColumnStats();
    }

    public Column(String name, PrimitiveType dataType) {
        this(name, new ColumnType(dataType, -1, -1, -1), false, null, false, null, "");
    }
    
    public Column(String name, ColumnType columnType) {
        this(name, columnType, false, null, false, null, "");
    }

    public Column(String name, ColumnType columnType, boolean isKey, AggregateType aggregateType, String defaultValue,
                  String comment) {
        this(name, columnType, isKey, aggregateType, false, defaultValue, comment);
    }

    public Column(String name, ColumnType columnType, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
                  String defaultValue, String comment) {
        this.name = name;
        if (this.name == null) {
            this.name = "";
        }

        this.columnType = columnType;
        if (this.columnType == null) {
            this.columnType = new ColumnType();
        }

        this.aggregationType = aggregateType;
        this.isAggregationTypeImplicit = false;
        this.isKey = isKey;
        this.isAllowNull = isAllowNull;
        this.defaultValue = defaultValue;
        this.comment = comment;

        this.stats = new ColumnStats();
    }

    public Column(Column column) {
        this.name = column.getName();
        this.columnType = column.getColumnType();
        this.aggregationType = column.getAggregationType();
        this.isAggregationTypeImplicit = column.isAggregationTypeImplicit();
        this.isKey = column.isKey();
        this.isAllowNull = column.isAllowNull();
        this.defaultValue = column.getDefaultValue();
        this.comment = column.getComment();
        this.stats = column.getStats();
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public void setVarcharLimit(boolean value) {
        if (this.columnType.getType() == PrimitiveType.VARCHAR) {
            this.columnType.setVarcharLimit(value);
        }
    }
    public String getName() {
        return this.name;
    }

    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    public boolean isKey() {
        return this.isKey;
    }

    public ColumnType getColumnType() {
        return this.columnType;
    }

    public PrimitiveType getDataType() {
        return this.columnType.getType();
    }

    // TODO(zc):
    public Type getType() {
        return ScalarType.createType(columnType.getType());
    }

    public int getStrLen() {
        return this.columnType.getLen();
    }
    
    public int getPrecision() {
        return this.columnType.getPrecision();
    }
    
    public int getScale() {
        return this.columnType.getScale();
    }

    public AggregateType getAggregationType() {
        return this.aggregationType;
    }

    public boolean isAggregationTypeImplicit() {
        return this.isAggregationTypeImplicit;
    }

    public void setAggregationType(AggregateType aggregationType, boolean isAggregationTypeImplicit) {
        this.aggregationType = aggregationType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
    }

    public boolean isAllowNull() {
        return isAllowNull;
    }

    public String getDefaultValue() {
        return this.defaultValue;
    }

    public void setStats(ColumnStats stats) {
        this.stats = stats;
    }
    
    public ColumnStats getStats() {
        return this.stats;
    }
    
    public String getComment() {
        return comment;
    }

    public void analyze(boolean isOlap) throws AnalysisException {
        if (name == null || columnType == null) {
            throw new AnalysisException("No column name or column type in column definition.");
        }

        FeNameFormat.checkColumnName(name);

        columnType.analyze();

        if (aggregationType != null) {
            // check if aggregate type is valid
            if (!aggregationType.checkCompatibility(columnType.getType())) {
                throw new AnalysisException(String.format("Aggregate type %s is not compatible with primitive type %s",
                                                          toString(), columnType.toSql()));
            }
        }

        if (columnType.getType() == PrimitiveType.FLOAT || columnType.getType() == PrimitiveType.DOUBLE) {
            if (isOlap && isKey) {
                throw new AnalysisException("Float or double can't be used as a key, use decimal instead.");
            }
        }

        if (columnType.getType() == PrimitiveType.HLL) {
            if (defaultValue != null) {
                throw new AnalysisException("Hll can not set default value");
            }
            defaultValue = HLL_EMPTY_SET;
        } 

        if (defaultValue != null) {
            validateDefaultValue(columnType, defaultValue);
        }
    }

    public static void validateDefaultValue(ColumnType columnType, String defaultValue) throws AnalysisException {
        Preconditions.checkNotNull(defaultValue);

        // check if default value is valid.
        // if not, some literal constructor will throw AnalysisException
        PrimitiveType type = columnType.getType();
        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                IntLiteral intLiteral = new IntLiteral(defaultValue, Type.fromPrimitiveType(type));
                break;
            case LARGEINT:
                LargeIntLiteral largeIntLiteral = new LargeIntLiteral(defaultValue);
                largeIntLiteral.analyze(null);
                break;
            case FLOAT:
                FloatLiteral floatLiteral = new FloatLiteral(defaultValue);
                if (floatLiteral.getType() == Type.DOUBLE) {
                    throw new AnalysisException("Default value will loose precision: " + defaultValue);
                }
            case DOUBLE:
                FloatLiteral doubleLiteral = new FloatLiteral(defaultValue);
                break;
            case DECIMAL:
                DecimalLiteral decimalLiteral = new DecimalLiteral(defaultValue);
                decimalLiteral.checkPrecisionAndScale(columnType.getPrecision(), columnType.getScale());
                break;
            case DATE:
            case DATETIME:
                DateLiteral dateLiteral = new DateLiteral(defaultValue, Type.fromPrimitiveType(type));
                break;
            case CHAR:
            case VARCHAR:
            case HLL:
                if (defaultValue.length() > columnType.getLen()) {
                    throw new AnalysisException("Default value is too long: " + defaultValue);
                }
                break;
            default:
                throw new AnalysisException("Unsupported type: " + columnType);
        }
    }

    public int getOlapColumnIndexSize() {
        PrimitiveType type = this.getDataType();
        if (type == PrimitiveType.CHAR) {
            return columnType.getLen();
        } else {
            return type.getOlapColumnIndexSize();
        }
    }

    public TColumn toThrift() {
        TColumn tColumn = new TColumn();
        tColumn.setColumn_name(this.name);

        TColumnType tColumnType = new TColumnType();
        tColumnType.setType(this.getDataType().toThrift());
        tColumnType.setLen(this.getStrLen());
        tColumnType.setPrecision(this.getPrecision());
        tColumnType.setScale(this.getScale());

        tColumnType.setIndex_len(this.getOlapColumnIndexSize());

        tColumn.setColumn_type(tColumnType);
        if (null != this.aggregationType) {
            tColumn.setAggregation_type(this.aggregationType.toThrift());
        }
        tColumn.setIs_key(this.isKey);
        tColumn.setIs_allow_null(this.isAllowNull);
        tColumn.setDefault_value(this.defaultValue);
        return tColumn;
    }

    public void checkSchemaChangeAllowed(Column other) throws DdlException {
        if (Strings.isNullOrEmpty(other.name)) {
            throw new DdlException("Dest column name is empty");
        }

        if (!this.columnType.isSchemaChangeAllowed(other.columnType)) {
            throw new DdlException("Cannot change " + getDataType() + " to " + other.getDataType());
        }

        if (this.aggregationType != other.aggregationType) {
            throw new DdlException("Cannot change aggregation type");
        }

        if (this.isAllowNull && !other.isAllowNull) {
            throw new DdlException("Cannot change from null to not null");
        }

        if (this.getDefaultValue() == null) {
            if (other.getDefaultValue() != null) {
                throw new DdlException("Cannot change default value");
            }
        } else {
            if (!this.getDefaultValue().equals(other.getDefaultValue())) {
                throw new DdlException("Cannot change default value");
            }
        }

        if ((getDataType() == PrimitiveType.VARCHAR && other.getDataType() == PrimitiveType.VARCHAR)
                || (getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.VARCHAR)
                || (getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.CHAR)) {
            if (getStrLen() >= other.getStrLen()) {
                throw new DdlException("Cannot shorten string length");
            }
        }

        if (this.getPrecision() != other.getPrecision()) {
            throw new DdlException("Cannot change precision");
        }

        if (this.getScale() != other.getScale()) {
            throw new DdlException("Cannot change scale");
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        sb.append(columnType.toSql()).append(" ");
        if (aggregationType != null && !isAggregationTypeImplicit) {
            sb.append(aggregationType.name()).append(" ");
        }
        if (isAllowNull) {
            sb.append("NULL ");
        } else {
            sb.append("NOT NULL ");
        }
        if (defaultValue != null) {
            sb.append("DEFAULT \"").append(defaultValue).append("\" ");
        }
        sb.append("COMMENT \"").append(comment).append("\"");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Column)) {
            return false;
        }

        Column other = (Column) obj;

        if (!this.name.equalsIgnoreCase(other.getName())) {
            return false;
        }
        if (this.getDataType() != other.getDataType()) {
            return false;
        }
        if (this.aggregationType != other.getAggregationType()) {
            return false;
        }
        if (this.isAggregationTypeImplicit != other.isAggregationTypeImplicit()) {
            return false;
        }
        if (this.isKey != other.isKey()) {
            return false;
        }
        if (this.isAllowNull != other.isAllowNull) {
            return false;
        }
        if (this.getDefaultValue() == null) {
            if (other.getDefaultValue() != null) {
                return false;
            }
        } else {
            if (!this.getDefaultValue().equals(other.getDefaultValue())) {
                return false;
            }
        }

        if (this.getStrLen() != other.getStrLen()) {
            return false;
        }
        if (this.getPrecision() != other.getPrecision()) {
            return false;
        }
        if (this.getScale() != other.getScale()) {
            return false;
        }
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, name);
        columnType.write(out);
        if (null == aggregationType) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, aggregationType.name());
            out.writeBoolean(isAggregationTypeImplicit);
        }

        out.writeBoolean(isKey);
        out.writeBoolean(isAllowNull);
        
        if (defaultValue == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, defaultValue);
        }
        stats.write(out);

        Text.writeString(out, comment);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = Text.readString(in);
        columnType = new ColumnType();
        columnType.readFields(in);
        boolean notNull = in.readBoolean();
        if (notNull) {
            aggregationType = AggregateType.valueOf(Text.readString(in));

            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
                isAggregationTypeImplicit = in.readBoolean();
            } else {
                isAggregationTypeImplicit = false;
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            isKey = in.readBoolean();
        } else {
            isKey = (aggregationType == null);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            isAllowNull = in.readBoolean();
        } else {
            isAllowNull = false;
        }

        notNull = in.readBoolean();
        if (notNull) {
            defaultValue = Text.readString(in);
        }
        stats = ColumnStats.read(in);

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_10) {
            comment = Text.readString(in);
        } else {
            comment = "";
        }
    }

    public static Column read(DataInput in) throws IOException {
        Column column = new Column();
        column.readFields(in);
        return column;
    }
}
