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
import org.apache.doris.datasource.hive.HiveMetaStoreCache;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class PartitionKey implements Comparable<PartitionKey>, Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionKey.class);
    private List<LiteralExpr> keys;
    private List<PrimitiveType> types;
    private boolean isDefaultListPartitionKey = false;

    // constructor for partition prune
    public PartitionKey() {
        keys = Lists.newArrayList();
        types = Lists.newArrayList();
    }

    public void setDefaultListPartition(boolean isDefaultListPartitionKey) {
        this.isDefaultListPartitionKey = isDefaultListPartitionKey;
    }

    public boolean isDefaultListPartitionKey() {
        return isDefaultListPartitionKey;
    }

    // Factory methods
    public static PartitionKey createInfinityPartitionKey(List<Column> columns, boolean isMax)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        for (Column column : columns) {
            partitionKey.keys.add(LiteralExpr.createInfinity(column.getType(), isMax));
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
            partitionKey.keys.add(keys.get(i).getValue(columns.get(i).getType()));
            partitionKey.types.add(columns.get(i).getDataType());
        }

        // fill the vacancy with MIN
        for (; i < columns.size(); ++i) {
            partitionKey.keys.add(LiteralExpr.createInfinity(columns.get(i).getType(), false));
            partitionKey.types.add(columns.get(i).getDataType());
        }

        Preconditions.checkState(partitionKey.keys.size() == columns.size());
        return partitionKey;
    }

    public static PartitionKey createListPartitionKeyWithTypes(List<PartitionValue> values, List<Type> types)
            throws AnalysisException {
        // for multi list partition:
        //
        // PARTITION BY LIST(k1, k2)
        // (
        //     PARTITION p1 VALUES IN (("1","beijing"), ("1", "shanghai")),
        //     PARTITION p2 VALUES IN (("2","shanghai")),
        //     PARTITION p3 VALUES IN,
        //     PARTITION p4,
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
        //     PARTITION p6 VALUES IN ("26"),
        //     PARTITION p5 VALUES IN,
        //     PARTITION p7
        // )
        //
        // ListPartitionInfo::createAndCheckPartitionItem has checked
        Preconditions.checkArgument(values.size() <= types.size(),
                "in value size[" + values.size() + "] is not less than partition column size[" + types.size() + "].");

        PartitionKey partitionKey = new PartitionKey();
        for (int i = 0; i < values.size(); i++) {
            partitionKey.keys.add(values.get(i).getValue(types.get(i)));
            partitionKey.types.add(types.get(i).getPrimitiveType());
        }
        if (values.isEmpty()) {
            for (int i = 0; i < types.size(); ++i) {
                partitionKey.keys.add(LiteralExpr.createInfinity(types.get(i), false));
                partitionKey.types.add(types.get(i).getPrimitiveType());
            }
            partitionKey.setDefaultListPartition(true);
        }

        Preconditions.checkState(partitionKey.keys.size() == types.size());
        return partitionKey;
    }

    public static PartitionKey createListPartitionKey(List<PartitionValue> values, List<Column> columns)
            throws AnalysisException {
        List<Type> types = columns.stream().map(c -> c.getType()).collect(Collectors.toList());
        return createListPartitionKeyWithTypes(values, types);
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

    public List<PrimitiveType> getTypes() {
        return types;
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

    public List<String> getPartitionValuesAsStringList() {
        return keys.stream().map(k -> k.getStringValue()).collect(Collectors.toList());
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
        int thisKeyLen = this.keys.size();
        int otherKeyLen = other.keys.size();
        int minLen = Math.min(thisKeyLen, otherKeyLen);
        for (int i = 0; i < minLen; ++i) {
            int ret = compareLiteralExpr(this.getKeys().get(i), other.getKeys().get(i));
            if (0 != ret) {
                return ret;
            }
        }
        return Integer.compare(thisKeyLen, otherKeyLen);
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
        if (isDefaultListPartitionKey()) {
            builder.append("default key");
        } else {
            builder.append(toString(keys));
        }
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
                    case DATEV2:
                    case DATETIMEV2:
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
            if (type != PrimitiveType.DATETIMEV2) {
                literal.setType(Type.fromPrimitiveType(type));
            }
            if (type.isDateV2Type()) {
                try {
                    literal.checkValueValid();
                } catch (AnalysisException e) {
                    LOG.warn("Value {} for partition key [type = {}] is invalid! This is a bug exists in Doris "
                            + "1.2.0 and fixed since Doris 1.2.1. You should create this table again using Doris "
                            + "1.2.1+ .", literal.getStringValue(), type);
                    ((DateLiteral) literal).setMinValue();
                }
            }
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
        int hashCode = 1;
        for (LiteralExpr key : keys) {
            hashCode = 31 * hashCode + (key == null ? 0 : key.hashCode());
        }
        int ret = types.size() * 1000;
        for (PrimitiveType type : types) {
            ret += type.ordinal();
        }
        return hashCode + ret;
    }

    public static class PartitionKeySerializer implements JsonSerializer<PartitionKey> {
        @Override
        public JsonElement serialize(PartitionKey partitionKey, java.lang.reflect.Type reflectType,
                                     JsonSerializationContext context) {
            JsonArray result = new JsonArray();

            List<PrimitiveType> types = partitionKey.getTypes();
            List<LiteralExpr> keys = partitionKey.getKeys();
            int count = keys.size();
            if (count != types.size()) {
                throw new JsonParseException("Size of keys and types are not equal");
            }

            for (int i = 0; i < count; i++) {
                JsonArray typeAndKey = new JsonArray();
                PrimitiveType type = types.get(i);
                typeAndKey.add(new JsonPrimitive(type.toString()));

                if (keys.get(i) == MaxLiteral.MAX_VALUE) {
                    typeAndKey.add(new JsonPrimitive("MAX_VALUE"));
                } else {
                    switch (type) {
                        case TINYINT:
                        case SMALLINT:
                        case INT:
                        case BIGINT: {
                            IntLiteral key = (IntLiteral) keys.get(i);
                            typeAndKey.add(new JsonPrimitive(key.getLongValue()));
                        }
                            break;
                        case LARGEINT: {
                            LargeIntLiteral key = (LargeIntLiteral) keys.get(i);
                            typeAndKey.add(new JsonPrimitive(key.getRealValue().toString()));
                        }
                            break;
                        case DATE:
                        case DATETIME:
                        case DATEV2:
                        case DATETIMEV2: {
                            DateLiteral key = (DateLiteral) keys.get(i);
                            typeAndKey.add(new JsonPrimitive(key.convertToString(type)));
                        }
                            break;
                        case CHAR:
                        case VARCHAR:
                        case STRING: {
                            StringLiteral key = (StringLiteral) keys.get(i);
                            typeAndKey.add(new JsonPrimitive(key.getValue()));
                        }
                            break;
                        case BOOLEAN: {
                            BoolLiteral key = (BoolLiteral) keys.get(i);
                            typeAndKey.add(new JsonPrimitive(key.getValue()));
                        }
                            break;
                        default:
                            throw new JsonParseException("type[" + type.name() + "] not supported: ");
                    }
                }

                result.add(typeAndKey);
            }

            return result;
        }
    }

    // if any of partition value is HIVE_DEFAULT_PARTITION
    // return true to indicate that this is a hive default partition
    public boolean isHiveDefaultPartition() {
        for (LiteralExpr literalExpr : keys) {
            if (!(literalExpr instanceof StringLiteral)) {
                continue;
            }
            StringLiteral key = (StringLiteral) literalExpr;
            if (key.getValue().equals(HiveMetaStoreCache.HIVE_DEFAULT_PARTITION)) {
                return true;
            }
        }
        return false;
    }
}
