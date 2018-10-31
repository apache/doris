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

package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TColumnType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 这个是对Column类型的一个封装，对于大多数类型，primitive type足够了，这里有两个例外需要用到这个信息
 * 1. 对于decimal，character这种有一些附加信息的
 * 2. 如果在未来需要增加嵌套类型，那么这个ColumnType就是必须的了
 */
public class ColumnType implements Writable {
    private static final int VAR_CHAR_UPPER_LIMIT = 65533;
    private static Boolean[][] schemaChangeMatrix;

    static {
        schemaChangeMatrix = new Boolean[PrimitiveType.BINARY.ordinal() + 1][PrimitiveType.BINARY.ordinal() + 1];

        for (int i = 0; i < schemaChangeMatrix.length; i++) {
            for (int j = 0; j < schemaChangeMatrix[i].length; j++) {
                if (i == j) {
                    schemaChangeMatrix[i][j] = true;
                } else {
                    schemaChangeMatrix[i][j] = false;
                }
            }
        }

        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.SMALLINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.TINYINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.INT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.SMALLINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.BIGINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.INT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.LARGEINT.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.BIGINT.ordinal()][PrimitiveType.DOUBLE.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.VARCHAR.ordinal()] = true;
        schemaChangeMatrix[PrimitiveType.CHAR.ordinal()][PrimitiveType.CHAR.ordinal()] = true;

        schemaChangeMatrix[PrimitiveType.DATE.ordinal()][PrimitiveType.DATETIME.ordinal()] = true;
    }

    private PrimitiveType type;

    // Unused if type is always the same length.
    private int len;

    // Used for decimal(precision, scale)
    // precision: maximum number of digits
    // scale: the number of digits to the right of the decimal point
    private int precision;
    private int scale;
    // used for limiting varchar size
    private boolean varcharLimit = true;

    private volatile Type typeDesc;

    public ColumnType() {
        this.type = PrimitiveType.NULL_TYPE;
    }

    public ColumnType(PrimitiveType type) {
        this(type, -1, -1, -1);
    }

    public ColumnType(PrimitiveType type, int len, int precision, int scale) {
        this.type = type;
        this.len = len;
        this.precision = precision;
        this.scale = scale;
        if (this.type == null) {
            this.type = PrimitiveType.NULL_TYPE;
        }
    }

    // This is used for built-in function to create intermediate type
    public static ColumnType createInterType(PrimitiveType type) {
        switch (type) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case LARGEINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATETIME:
                return createType(type);
            case DECIMAL:
                return createDecimal(27, 9);
            case CHAR:
            case VARCHAR:
                return createVarchar(64);
            case HLL:
                return createHll();
            default:
                return null;
        }
    }

    public static ColumnType createType(PrimitiveType type) {
        return new ColumnType(type);
    }

    public static ColumnType createVarchar(int len) {
        ColumnType type = new ColumnType(PrimitiveType.VARCHAR);
        type.len = len;
        return type;
    }
    
    public static ColumnType createHll() {
        ColumnType type = new ColumnType(PrimitiveType.HLL);
        type.len = ScalarType.MAX_HLL_LENGTH;
        return type;
    }

    // Create varchar type
    public static ColumnType createChar(int len) {
        ColumnType type = new ColumnType(PrimitiveType.CHAR);
        type.len = len;
        return type;
    }

    public static ColumnType createDecimal(int precision, int scale) {
        ColumnType type = new ColumnType(PrimitiveType.DECIMAL);
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public PrimitiveType getType() {
        return type;
    }

    public Type getTypeDesc() {
        if (typeDesc != null) {
            return typeDesc;
        }
        switch (type) {
            case VARCHAR:
                typeDesc = ScalarType.createVarcharType(len);
                break;
            case CHAR:
                typeDesc = ScalarType.createCharType(len);
                break;
            case DECIMAL:
                typeDesc = ScalarType.createDecimalType(precision, scale);
                break;
            default:
                typeDesc = ScalarType.createType(type);
                break;
        }
        return typeDesc;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public void setVarcharLimit(boolean value) {
        this.varcharLimit = value;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public boolean isString() {
        return type == PrimitiveType.CHAR || type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL;
    }

    public int getMemlayoutBytes() {
        switch (type) {
            case BOOLEAN:
                return 0;
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
                return 4;
            case BIGINT:
                return 8;
            case LARGEINT:
                return 16;
            case FLOAT:
                return 4;
            case DOUBLE:
                return 12;
            case DATE:
                return 3;
            case DATETIME:
                return 8;
            case DECIMAL:
                return 40;
            case CHAR:
            case VARCHAR:
                return len;
            case HLL:
                return 16385;
            default:
                return 0;
        }
    }

    public void analyze() throws AnalysisException {
        if (type == PrimitiveType.INVALID_TYPE) {
            throw new AnalysisException("Invalid type.");
        }

        // check parameter valid
        switch (type) {
            case CHAR:
                if (len <= 0 || len > 255) {
                    throw new AnalysisException("Char size must between 1~255."
                            + " Size was set to: " + len + ".");
                }
                break;
            case VARCHAR:
                if (varcharLimit) {
                    if (len <= 0 || len > VAR_CHAR_UPPER_LIMIT) {
                        throw new AnalysisException("when engine=olap, varchar size must between 1~65533."
                                + " Size was set to: " + len + ".");
                    }
                } else {
                    if (len <= 0) {
                        throw new AnalysisException("When engine=mysql, varchar size must be great than 1."); 
                    }
                }
                break;
            case HLL:
                if (len <= 0 || len > 65533) {
                    throw new AnalysisException("Hll size must between 1~65533."
                            + " Size was set to: " + len + ".");
                }
                break;
            case DECIMAL:
                // precision: [1, 27]
                if (precision < 1 || precision > 27) {
                    throw new AnalysisException("Precision of decimal must between 1 and 27."
                            + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > 9) {
                    throw new AnalysisException("Scale of decimal must between 0 and 9."
                            + " Scale was set to: " + scale + ".");
                }
                // scale < precision
                if (scale >= precision) {
                    throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + scale + " and precision is " + precision);
                }
                break;
            default:
                // do nothing
        }
    }


    public boolean isSchemaChangeAllowed(ColumnType other) {
        return schemaChangeMatrix[type.ordinal()][other.type.ordinal()];
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        switch (type) {
            case CHAR:
                stringBuilder.append("char").append("(").append(len).append(")");
                break;
            case VARCHAR:
                stringBuilder.append("varchar").append("(").append(len).append(")");
                break;
            case DECIMAL:
                stringBuilder.append("decimal").append("(").append(precision).append(", ").append(scale).append(")");
                break;
            case BOOLEAN:
                stringBuilder.append("tinyint(1)");
                break;
            case TINYINT:
                stringBuilder.append("tinyint(4)");
                break;
            case SMALLINT:
                stringBuilder.append("smallint(6)");
                break;
            case INT:
                stringBuilder.append("int(11)");
                break;
            case BIGINT:
                stringBuilder.append("bigint(20)");
                break;
            case LARGEINT:
                stringBuilder.append("largeint(40)");
                break;
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATETIME:
            case HLL:
                stringBuilder.append(type.toString().toLowerCase());
                break;
            default:
                stringBuilder.append("unknown");
                break;
        }
        return stringBuilder.toString();
    }

    public TColumnType toThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = type.toThrift();
        if (type == PrimitiveType.CHAR || type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) {
            thrift.setLen(len);
        }
        if (type == PrimitiveType.DECIMAL) {
            thrift.setPrecision(precision);
            thrift.setScale(scale);
        }
        return thrift;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ColumnType)) {
            return false;
        }
        ColumnType other = (ColumnType) o;
        if (type != other.type) {
            return false;
        }
        if (type == PrimitiveType.DECIMAL) {
            return scale == other.scale && precision == other.precision;
        } else if (type == PrimitiveType.CHAR) {
            return len == other.len;
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
        out.writeInt(scale);
        out.writeInt(precision);
        out.writeInt(len);
        out.writeBoolean(varcharLimit);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = PrimitiveType.valueOf(Text.readString(in));
        scale = in.readInt();
        precision = in.readInt();
        len = in.readInt();
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            varcharLimit = in.readBoolean();
        }
    }

}

