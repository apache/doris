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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

public class PartitionKey implements Comparable<PartitionKey>, Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionKey.class);
    private List<LiteralExpr> keys;
    private List<PrimitiveType> types;

    // constructor for partition prune
    public PartitionKey() {
        keys = Lists.newArrayList();
        types = Lists.newArrayList();
    }

    // Factory methods
    public static PartitionKey createInfinityPartitionKey(List<Column> columns, boolean isMax)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        for (Column column : columns) {
            partitionKey.keys.add(LiteralExpr.createInfinity(Type.fromPrimitiveType(column.getDataType()), isMax));
            partitionKey.types.add(column.getDataType());
        }
        return partitionKey;
    }

    public static PartitionKey createPartitionKey(List<PartitionValue> keys, List<Column> columns)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        Preconditions.checkArgument(keys.size() <= columns.size());
        int i;
        for (i = 0; i < keys.size(); ++i) {
            partitionKey.keys.add(keys.get(i).getValue(
                    Type.fromPrimitiveType(columns.get(i).getDataType())));
            partitionKey.types.add(columns.get(i).getDataType());
        }

        // fill the vacancy with MIN
        for (; i < columns.size(); ++i) {
            Type type = Type.fromPrimitiveType(columns.get(i).getDataType());
            partitionKey.keys.add(LiteralExpr.createInfinity(type, false));
            partitionKey.types.add(columns.get(i).getDataType());
        }

        Preconditions.checkState(partitionKey.keys.size() == columns.size());
        return partitionKey;
    }

    public static PartitionKey createListPartitionKey(List<PartitionValue> values, List<Column> columns)
            throws AnalysisException {
        // for multi list partition:
        //
        // PARTITION BY LIST(k1, k2)
        // (
        //     PARTITION p1 VALUES IN (("1","beijing"), ("1", "shanghai")),
        //     PARTITION p2 VALUES IN (("2","shanghai"))
        // )
        //
        // for single list partition:
        //
        // PARTITION BY LIST(`k1`)
        // (
        //     PARTITION p1 VALUES IN ("1", "2", "3", "4", "5"),
        //     PARTITION p2 VALUES IN ("6", "7", "8", "9", "10"),
        //     PARTITION p3 VALUES IN ("11", "12", "13", "14", "15"),
        //     PARTITION p4 VALUES IN ("16", "17", "18", "19", "20"),
        //     PARTITION p5 VALUES IN ("21", "22", "23", "24", "25"),
        //     PARTITION p6 VALUES IN ("26")
        // )
        //
        Preconditions.checkArgument(values.size() == columns.size(),
                "in value size[" + values.size() + "] is not equal to partition column size[" + columns.size() + "].");

        PartitionKey partitionKey = new PartitionKey();
        for (int i = 0; i < values.size(); i++) {
            partitionKey.keys.add(values.get(i).getValue(Type.fromPrimitiveType(columns.get(i).getDataType())));
            partitionKey.types.add(columns.get(i).getDataType());
        }

        return partitionKey;
    }

    public void pushColumn(LiteralExpr keyValue, PrimitiveType keyType) {
        keys.add(keyValue);
        types.add(keyType);
    }

    public void popColumn() {
        keys.remove(keys.size() - 1);
        types.remove(types.size() - 1);
    }

    public List<LiteralExpr> getKeys() {
        return keys;
    }

    public long getHashValue() {
        CRC32 hashValue = new CRC32();
        int i = 0;
        for (LiteralExpr expr : keys) {
            ByteBuffer buffer = expr.getHashValue(types.get(i));
            hashValue.update(buffer.array(), 0, buffer.limit());
            i++;
        }
        return hashValue.getValue();
    }

    public boolean isMinValue() {
        for (LiteralExpr literalExpr : keys) {
            if (!literalExpr.isMinValue()) {
                return false;
            }
        }
        return true;
    }

    public boolean isMaxValue() {
        for (LiteralExpr literalExpr : keys) {
            if (literalExpr != MaxLiteral.MAX_VALUE) {
                return false;
            }
        }
        return true;
    }

    public static int compareLiteralExpr(LiteralExpr key1, LiteralExpr key2) {
        int ret = 0;
        if (key1 instanceof MaxLiteral || key2 instanceof MaxLiteral) {
            ret = key1.compareLiteral(key2);
        } else {
            final Type destType = Type.getAssignmentCompatibleType(key1.getType(), key2.getType(), false);
            try {
                LiteralExpr newKey = key1;
                if (key1.getType() != destType) {
                    newKey = (LiteralExpr) key1.castTo(destType);
                }
                LiteralExpr newOtherKey = key2;
                if (key2.getType() != destType) {
                    newOtherKey = (LiteralExpr) key2.castTo(destType);
                }
                ret = newKey.compareLiteral(newOtherKey);
            } catch (AnalysisException e) {
                throw new RuntimeException("Cast error in partition");
            }
        }
        return ret;
    }

    // compare with other PartitionKey. used for partition prune
    @Override
    public int compareTo(PartitionKey other) {
        int this_key_len = this.keys.size();
        int other_key_len = other.keys.size();
        int min_len = Math.min(this_key_len, other_key_len);
        for (int i = 0; i < min_len; ++i) {
            int ret = compareLiteralExpr(this.getKeys().get(i), other.getKeys().get(i));
            if (0 != ret) {
                return ret;
            }
        }
        return Integer.compare(this_key_len, other_key_len);
    }

    // return: ("100", "200", "300")
    public String toSql() {
        StringBuilder sb = new StringBuilder("(");
        int i = 0;
        for (LiteralExpr expr : keys) {
            Object value = null;
            if (expr == MaxLiteral.MAX_VALUE) {
                value = expr.toSql();
                sb.append(value);
                continue;
            } else {
                value = "\"" + expr.getRealValue() + "\"";
                if (expr instanceof DateLiteral) {
                    DateLiteral dateLiteral = (DateLiteral) expr;
                    value = dateLiteral.toSql();
                }
            }
            sb.append(value);

            if (keys.size() - 1 != i) {
                sb.append(", ");
            }
            i++;
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("types: [");
        builder.append(Joiner.on(", ").join(types));
        builder.append("]; ");

        builder.append("keys: [");
        builder.append(toString(keys));
        builder.append("]; ");

        return builder.toString();
    }

    public static String toString(List<LiteralExpr> keys) {
        StringBuilder builder = new StringBuilder();
        int i = 0;
        for (LiteralExpr expr : keys) {
            Object value = null;
            if (expr == MaxLiteral.MAX_VALUE) {
                value = expr.toSql();
            } else {
                value = expr.getRealValue();
                if (expr instanceof DateLiteral) {
                    DateLiteral dateLiteral = (DateLiteral) expr;
                    value = dateLiteral.getStringValue();
                }
            }
            if (keys.size() - 1 == i) {
                builder.append(value);
            } else {
                builder.append(value).append(", ");
            }
            ++i;
        }
        return builder.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = keys.size();
        if (count != types.size()) {
            throw new IOException("Size of keys and types are not equal");
        }

        out.writeInt(count);
        for (int i = 0; i < count; i++) {
            PrimitiveType type = types.get(i);
            Text.writeString(out, type.toString());
            if (keys.get(i) == MaxLiteral.MAX_VALUE) {
                out.writeBoolean(true);
            } else {
                out.writeBoolean(false);
                keys.get(i).write(out);
            }
        }
    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            PrimitiveType type = PrimitiveType.valueOf(Text.readString(in));
            types.add(type);

            LiteralExpr literal = null;
            boolean isMax = in.readBoolean();
            if (isMax) {
                literal = MaxLiteral.MAX_VALUE;
            } else {
                switch (type) {
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                        literal = IntLiteral.read(in);
                        break;
                    case LARGEINT:
                        literal = LargeIntLiteral.read(in);
                        break;
                    case DATE:
                    case DATETIME:
                        literal = DateLiteral.read(in);
                        break;
                    case CHAR:
                    case VARCHAR:
                    case STRING:
                        literal = StringLiteral.read(in);
                        break;
                    case BOOLEAN:
                        literal = BoolLiteral.read(in);
                        break;
                    default:
                        throw new IOException("type[" + type.name() + "] not supported: ");
                }
            }
            literal.setType(Type.fromPrimitiveType(type));
            keys.add(literal);
        }
    }

    public static PartitionKey read(DataInput in) throws IOException {
        PartitionKey key = new PartitionKey();
        key.readFields(in);
        return key;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PartitionKey)) {
            return false;
        }

        PartitionKey partitionKey = (PartitionKey) obj;

        // Check keys
        if (keys != partitionKey.keys) {
            if (keys.size() != partitionKey.keys.size()) {
                return false;
            }
            for (int i = 0; i < keys.size(); i++) {
                if (!keys.get(i).equals(partitionKey.keys.get(i))) {
                    return false;
                }
            }
        }

        // Check types
        if (types != partitionKey.types) {
            if (types.size() != partitionKey.types.size()) {
                return false;
            }
            for (int i = 0; i < types.size(); i++) {
                if (!types.get(i).equals(partitionKey.types.get(i))) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int ret = types.size() * 1000;
        for (PrimitiveType type : types) {
            ret += type.ordinal();
        }
        return ret;
    }
}
