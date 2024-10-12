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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class PartitionKey implements Comparable<PartitionKey>, Writable {
    private static final Logger LOG = LogManager.getLogger(PartitionKey.class);
    @SerializedName("ks")
    private List<LiteralExpr> keys;
    private List<String> originHiveKeys;
    @SerializedName("ts")
    private List<PrimitiveType> types;
    @SerializedName("isD")
    private boolean isDefaultListPartitionKey = false;

    // constructor for partition prune
    public PartitionKey() {
        keys = Lists.newArrayList();
        originHiveKeys = Lists.newArrayList();
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

    public static PartitionKey createMaxPartitionKey() {
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.keys.add(MaxLiteral.MAX_VALUE);
        // type not set
        return partitionKey;
    }

    public static PartitionKey createPartitionKey(List<PartitionValue> keys, List<Column> columns)
            throws AnalysisException {
        PartitionKey partitionKey = new PartitionKey();
        Preconditions.checkArgument(keys.size() <= columns.size());
        int i;
        for (i = 0; i < keys.size(); ++i) {
            Type keyType = columns.get(i).getType();
            // If column type is datatime and key type is date, we should convert date to datetime.
            // if it's max value, no need to parse.
            if (!keys.get(i).isMax() && (keyType.isDatetime() || keyType.isDatetimeV2())) {
                Literal dateTimeLiteral = getDateTimeLiteral(keys.get(i).getStringValue(), keyType);
                partitionKey.keys.add(dateTimeLiteral.toLegacyLiteral());
            } else {
                partitionKey.keys.add(keys.get(i).getValue(keyType));
            }
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

    private static Literal getDateTimeLiteral(String value, Type type) throws AnalysisException {
        if (type.isDatetime()) {
            return new DateTimeLiteral(value);
        } else if (type.isDatetimeV2()) {
            return new DateTimeV2Literal(value);
        }
        throw new AnalysisException("date convert to datetime failed, "
                + "value is [" + value + "], type is [" + type + "].");
    }

    public static PartitionKey createListPartitionKeyWithTypes(List<PartitionValue> values, List<Type> types,
            boolean isHive)
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
            if (values.get(i).isNullPartition()) {
                partitionKey.keys.add(NullLiteral.create(types.get(i)));
            } else {
                partitionKey.keys.add(values.get(i).getValue(types.get(i)));
            }

            if (isHive) {
                partitionKey.originHiveKeys.add(values.get(i).getStringValue());
            }
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
        return createListPartitionKeyWithTypes(values, types, false);
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
        if (originHiveKeys.size() == keys.size()) {
            // for hive, we need ues originHiveKeys
            // because when a double 1.234 as partition column, it will save as '1.123000' for PartitionValue
            return getPartitionValuesAsStringListForHive();
        }
        return keys.stream().map(k -> k.getStringValue()).collect(Collectors.toList());
    }

    public List<String> getPartitionValuesAsStringListForHive() {
        Preconditions.checkState(originHiveKeys.size() == keys.size());
        return originHiveKeys;
    }

    public static int compareLiteralExpr(LiteralExpr key1, LiteralExpr key2) {
        int ret = 0;
        if (key1 instanceof MaxLiteral || key2 instanceof MaxLiteral) {
            ret = key1.compareLiteral(key2);
        } else {
            boolean enableDecimal256 = SessionVariable.getEnableDecimal256();
            final Type destType = Type.getAssignmentCompatibleType(key1.getType(), key2.getType(), false,
                    enableDecimal256);
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

    public PartitionKey successor() throws AnalysisException {
        Preconditions.checkState(
                keys.size() == 1,
                "Only support compute successor for one partition column"
        );
        LiteralExpr literal = keys.get(0);
        PrimitiveType type = types.get(0);

        PartitionKey successor = new PartitionKey();

        switch (type) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                long maxValueOfType = (1L << ((type.getSlotSize() << 3 /* multiply 8 bit */) - 1)) - 1L;
                long successorInt = ((IntLiteral) literal).getValue();
                successorInt += successorInt < maxValueOfType ? 1 : 0;
                successor.pushColumn(new IntLiteral(successorInt, Type.fromPrimitiveType(type)), type);
                return successor;
            case LARGEINT:
                BigInteger maxValue = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);
                BigInteger successorLargeInt = (BigInteger) literal.getRealValue();
                successorLargeInt = successorLargeInt.add(
                        successorLargeInt.compareTo(maxValue) < 0 ? BigInteger.ONE : BigInteger.ZERO
                );
                successor.pushColumn(new LargeIntLiteral(successorLargeInt), type);
                return successor;
            case DATE:
            case DATEV2:
            case DATETIME:
            case DATETIMEV2:
                DateLiteral dateLiteral = (DateLiteral) literal;
                LocalDateTime successorDateTime = LocalDateTime.of(
                        (int) dateLiteral.getYear(),
                        (int) dateLiteral.getMonth(),
                        (int) dateLiteral.getDay(),
                        (int) dateLiteral.getHour(),
                        (int) dateLiteral.getMinute(),
                        (int) dateLiteral.getSecond(),
                        (int) dateLiteral.getMicrosecond() * 1000
                );
                if (type == PrimitiveType.DATE || type == PrimitiveType.DATEV2) {
                    successorDateTime = successorDateTime.plusDays(1);
                } else if (type == PrimitiveType.DATETIME) {
                    successorDateTime = successorDateTime.plusSeconds(1);
                } else {
                    int scale = Math.min(6, Math.max(0, ((ScalarType) literal.getType()).getScalarScale()));
                    long nanoSeconds = BigInteger.TEN.pow(9 - scale).longValue();
                    successorDateTime = successorDateTime.plusNanos(nanoSeconds);
                }
                successor.pushColumn(new DateLiteral(successorDateTime, literal.getType()), type);
                return successor;
            default:
                throw new AnalysisException("Unsupported type: " + type);
        }
    }

    // return: ("100", "200", "300")
    public String toSql() {
        StringBuilder sb = new StringBuilder("(");
        int i = 0;
        for (LiteralExpr expr : keys) {
            Object value = null;
            if (expr == MaxLiteral.MAX_VALUE || expr.isNullLiteral()) {
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
        // ATTN: DO NOT EDIT unless unless you explicitly guarantee compatibility
        // between different versions.
        //
        // the ccr syncer depends on this string to identify partitions between two
        // clusters (cluster versions may be different).
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
            if (expr == MaxLiteral.MAX_VALUE || expr.isNullLiteral()) {
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

    // only used by ut
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            PrimitiveType type = PrimitiveType.valueOf(Text.readString(in).toUpperCase());
            boolean isMax = in.readBoolean();
            if (type == PrimitiveType.NULL_TYPE) {
                String realType = StringLiteral.read(in).getStringValue();
                type = PrimitiveType.valueOf(realType.toUpperCase());
                types.add(type);
                keys.add(NullLiteral.create(Type.fromPrimitiveType(type)));
                continue;
            }
            LiteralExpr literal = null;
            types.add(type);
            if (isMax) {
                literal = MaxLiteral.MAX_VALUE;
            } else {
                if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_133) {
                    literal = (LiteralExpr) GsonUtils.GSON.fromJson(Text.readString(in), Expr.class);
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
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_136) {
            PartitionKey key = new PartitionKey();
            key.readFields(in);
            return key;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), PartitionKey.class);
        }
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

    // added by ccr, and we have to follow.
    public static class PartitionKeySerializer
                implements JsonSerializer<PartitionKey>, JsonDeserializer<PartitionKey> {

        @Override
        public JsonElement serialize(PartitionKey partitionKey, java.lang.reflect.Type reflectType,
                                     JsonSerializationContext context) {
            // for compatibility
            List<PrimitiveType> types = partitionKey.getTypes();
            List<LiteralExpr> keys = partitionKey.getKeys();
            int count = keys.size();
            if (count != types.size()) {
                throw new JsonParseException("Size of keys and types are not equal");
            }

            JsonArray jsonArray = new JsonArray();
            for (int i = 0; i < count; i++) {
                JsonArray typeAndKey = new JsonArray();
                typeAndKey.add(context.serialize(types.get(i)));
                typeAndKey.add(context.serialize(keys.get(i)));
                jsonArray.add(typeAndKey);
            }

            // for compatibility in the future
            jsonArray.add(new JsonPrimitive("unused"));

            return jsonArray;
        }

        @Override
        public PartitionKey deserialize(JsonElement json, java.lang.reflect.Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
            if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_140) {
                return deserializeOld(json, typeOfT, context);
            } else {
                PartitionKey partitionKey = new PartitionKey();

                JsonArray jsonArray = json.getAsJsonArray();
                for (int i = 0; i < jsonArray.size() - 1; i++) {
                    PrimitiveType type = null;
                    type = context.deserialize(jsonArray.get(i).getAsJsonArray().get(0), PrimitiveType.class);
                    LiteralExpr key = context.deserialize(jsonArray.get(i).getAsJsonArray().get(1), Expr.class);

                    if (key instanceof NullLiteral) {
                        key = NullLiteral.create(Type.fromPrimitiveType(type));
                        partitionKey.types.add(type);
                        partitionKey.keys.add(key);
                        continue;
                    }
                    if (key instanceof MaxLiteral) {
                        key = MaxLiteral.MAX_VALUE;
                    }
                    if (type != PrimitiveType.DATETIMEV2) {
                        key.setType(Type.fromPrimitiveType(type));
                    }
                    if (type.isDateV2Type()) {
                        try {
                            key.checkValueValid();
                        } catch (AnalysisException e) {
                            LOG.warn("Value {} for partition key [type = {}] is invalid! This is a bug exists "
                                     + "in Doris 1.2.0 and fixed since Doris 1.2.1. You should create this table "
                                     + "again using Doris 1.2.1+ .", key.getStringValue(), type);
                            ((DateLiteral) key).setMinValue();
                        }
                    }

                    partitionKey.types.add(type);
                    partitionKey.keys.add(key);
                }

                // ignore the last element
                return partitionKey;
            }
        }

        // can be removed after 3.0.0
        private PartitionKey deserializeOld(JsonElement json, java.lang.reflect.Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
            PartitionKey partitionKey = new PartitionKey();
            JsonArray jsonArray = json.getAsJsonArray();
            for (int i = 0; i < jsonArray.size(); i++) {
                JsonArray typeAndKey = jsonArray.get(i).getAsJsonArray();
                PrimitiveType type = PrimitiveType.valueOf(typeAndKey.get(0).getAsString());
                if (type == PrimitiveType.NULL_TYPE) {
                    String realType = typeAndKey.get(1).getAsString();
                    type = PrimitiveType.valueOf(realType);
                    partitionKey.types.add(type);
                    partitionKey.keys.add(NullLiteral.create(Type.fromPrimitiveType(type)));
                    continue;
                }
                LiteralExpr literal = null;
                partitionKey.types.add(type);
                if (typeAndKey.get(1).getAsString().equals("MAX_VALUE")) {
                    literal = MaxLiteral.MAX_VALUE;
                } else {
                    switch (type) {
                        case TINYINT:
                        case SMALLINT:
                        case INT:
                        case BIGINT: {
                            long value = typeAndKey.get(1).getAsLong();
                            literal = new IntLiteral(value);
                        }
                            break;
                        case LARGEINT: {
                            String value = typeAndKey.get(1).getAsString();
                            try {
                                literal = new LargeIntLiteral(value);
                            } catch (AnalysisException e) {
                                throw new JsonParseException("LargeIntLiteral deserialize failed: " + e.getMessage());
                            }
                        }
                            break;
                        case DATE:
                        case DATETIME:
                        case DATEV2:
                        case DATETIMEV2: {
                            String value = typeAndKey.get(1).getAsString();
                            try {
                                literal = new DateLiteral(value, Type.fromPrimitiveType(type));
                            } catch (AnalysisException e) {
                                throw new JsonParseException("DateLiteral deserialize failed: " + e.getMessage());
                            }
                        }
                            break;
                        case CHAR:
                        case VARCHAR:
                        case STRING: {
                            String value = typeAndKey.get(1).getAsString();
                            literal = new StringLiteral(value);
                        }
                            break;
                        case BOOLEAN: {
                            boolean value = typeAndKey.get(1).getAsBoolean();
                            literal = new BoolLiteral(value);
                        }
                            break;
                        default:
                            throw new JsonParseException(
                                    "type[" + type.name() + "] not supported: ");
                    }
                }
                if (type != PrimitiveType.DATETIMEV2) {
                    literal.setType(Type.fromPrimitiveType(type));
                }

                partitionKey.keys.add(literal);
            }
            return partitionKey;
        }
    }

    // for test
    public List<String> getOriginHiveKeys() {
        return originHiveKeys;
    }
}
