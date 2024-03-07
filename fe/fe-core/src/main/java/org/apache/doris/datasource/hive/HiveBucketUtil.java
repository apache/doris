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

package org.apache.doris.datasource.hive;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.DdlException;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hive.common.util.Murmur3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveBucketUtil {
    private static final Logger LOG = LogManager.getLogger(HiveBucketUtil.class);

    private static final Set<PrimitiveType> SUPPORTED_TYPES_FOR_BUCKET_FILTER = ImmutableSet.of(
            PrimitiveType.BOOLEAN,
            PrimitiveType.TINYINT,
            PrimitiveType.SMALLINT,
            PrimitiveType.INT,
            PrimitiveType.BIGINT,
            PrimitiveType.STRING);

    private static PrimitiveTypeInfo convertToHiveColType(PrimitiveType dorisType) throws DdlException {
        switch (dorisType) {
            case BOOLEAN:
                return TypeInfoFactory.booleanTypeInfo;
            case TINYINT:
                return TypeInfoFactory.byteTypeInfo;
            case SMALLINT:
                return TypeInfoFactory.shortTypeInfo;
            case INT:
                return TypeInfoFactory.intTypeInfo;
            case BIGINT:
                return TypeInfoFactory.longTypeInfo;
            case STRING:
                return TypeInfoFactory.stringTypeInfo;
            default:
                throw new DdlException("Unsupported pruning bucket column type: " + dorisType);
        }
    }

    private static final Pattern BUCKET_WITH_OPTIONAL_ATTEMPT_ID_PATTERN =
            Pattern.compile("bucket_(\\d+)(_\\d+)?$");

    private static final Iterable<Pattern> BUCKET_PATTERNS = ImmutableList.of(
            // legacy Presto naming pattern (current version matches Hive)
            Pattern.compile("\\d{8}_\\d{6}_\\d{5}_[a-z0-9]{5}_bucket-(\\d+)(?:[-_.].*)?"),
            // Hive naming pattern per `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`
            Pattern.compile("(\\d+)_\\d+.*"),
            // Hive ACID with optional direct insert attempt id
            BUCKET_WITH_OPTIONAL_ATTEMPT_ID_PATTERN);

    public static List<InputSplit> getPrunedSplitsByBuckets(
            List<InputSplit> splits,
            String tableName,
            List<Expr> conjuncts,
            List<String> bucketCols,
            int numBuckets,
            Map<String, String> parameters) throws DdlException {
        Optional<Set<Integer>> prunedBuckets = HiveBucketUtil.getPrunedBuckets(
                conjuncts, bucketCols, numBuckets, parameters);
        if (!prunedBuckets.isPresent()) {
            return splits;
        }
        Set<Integer> buckets = prunedBuckets.get();
        if (buckets.size() == 0) {
            return Collections.emptyList();
        }
        List<InputSplit> result = new LinkedList<>();
        boolean valid = true;
        for (InputSplit split : splits) {
            String fileName = ((FileSplit) split).getPath().getName();
            OptionalInt bucket = getBucketNumberFromPath(fileName);
            if (bucket.isPresent()) {
                int bucketId = bucket.getAsInt();
                if (bucketId >= numBuckets) {
                    valid = false;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Hive table {} is corrupt for file {}(bucketId={}), skip bucket pruning.",
                                tableName, fileName, bucketId);
                    }
                    break;
                }
                if (buckets.contains(bucketId)) {
                    result.add(split);
                }
            } else {
                valid = false;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("File {} is not a bucket file in hive table {}, skip bucket pruning.",
                            fileName, tableName);
                }
                break;
            }
        }
        if (valid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("{} / {} input splits in hive table {} after bucket pruning.",
                        result.size(), splits.size(), tableName);
            }
            return result;
        } else {
            return splits;
        }
    }

    public static Optional<Set<Integer>> getPrunedBuckets(
            List<Expr> conjuncts, List<String> bucketCols, int numBuckets, Map<String, String> parameters)
            throws DdlException {
        if (parameters.containsKey("spark.sql.sources.provider")) {
            // spark currently does not populate bucketed output which is compatible with Hive.
            return Optional.empty();
        }
        int bucketVersion = Integer.parseInt(parameters.getOrDefault("bucketing_version", "1"));
        Optional<Set<Integer>> result = Optional.empty();
        for (Expr conjunct : conjuncts) {
            Optional<Set<Integer>> buckets = getPrunedBuckets(conjunct, bucketCols, bucketVersion, numBuckets);
            if (buckets.isPresent()) {
                if (!result.isPresent()) {
                    result = Optional.of(new HashSet<>(buckets.get()));
                } else {
                    result.get().retainAll(buckets.get());
                }
            }
        }
        return result;
    }

    public static Optional<Set<Integer>> getPrunedBuckets(
            Expr dorisExpr, List<String> bucketCols, int bucketVersion, int numBuckets) throws DdlException {
        // TODO(gaoxin): support multiple bucket columns
        if (dorisExpr == null || bucketCols == null || bucketCols.size() != 1) {
            return Optional.empty();
        }
        String bucketCol = bucketCols.get(0);
        if (dorisExpr instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) dorisExpr;
            Optional<Set<Integer>> result = Optional.empty();
            Optional<Set<Integer>> left = getPrunedBuckets(
                    compoundPredicate.getChild(0), bucketCols, bucketVersion, numBuckets);
            Optional<Set<Integer>> right = getPrunedBuckets(
                    compoundPredicate.getChild(1), bucketCols, bucketVersion, numBuckets);
            if (left.isPresent()) {
                result = Optional.of(new HashSet<>(left.get()));
            }
            switch (compoundPredicate.getOp()) {
                case AND: {
                    if (right.isPresent()) {
                        if (result.isPresent()) {
                            result.get().retainAll(right.get());
                        } else {
                            result = Optional.of(new HashSet<>(right.get()));
                        }
                    }
                    break;
                }
                case OR: {
                    if (right.isPresent()) {
                        if (result.isPresent()) {
                            result.get().addAll(right.get());
                        }
                    } else {
                        result = Optional.empty();
                    }
                    break;
                }
                default:
                    result = Optional.empty();
            }
            return result;
        } else if (dorisExpr instanceof  BinaryPredicate || dorisExpr instanceof InPredicate) {
            return pruneBucketsFromPredicate(dorisExpr, bucketCol, bucketVersion, numBuckets);
        } else {
            return Optional.empty();
        }
    }

    private static Optional<Set<Integer>> getPrunedBucketsFromLiteral(
            SlotRef slotRef, LiteralExpr literalExpr, String bucketCol, int bucketVersion, int numBuckets)
            throws DdlException {
        if (slotRef == null || literalExpr == null) {
            return Optional.empty();
        }
        String colName = slotRef.getColumnName();
        // check whether colName is bucket column or not
        if (!bucketCol.equals(colName)) {
            return Optional.empty();
        }
        PrimitiveType dorisPrimitiveType = slotRef.getType().getPrimitiveType();
        if (!SUPPORTED_TYPES_FOR_BUCKET_FILTER.contains(dorisPrimitiveType)) {
            return Optional.empty();
        }
        Object value = HiveMetaStoreClientHelper.extractDorisLiteral(literalExpr);
        if (value == null) {
            return Optional.empty();
        }
        PrimitiveObjectInspector constOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
                convertToHiveColType(dorisPrimitiveType).getPrimitiveCategory());
        PrimitiveObjectInspector origOI =
                PrimitiveObjectInspectorFactory.getPrimitiveObjectInspectorFromClass(value.getClass());
        Converter conv = ObjectInspectorConverters.getConverter(origOI, constOI);
        if (conv == null) {
            return Optional.empty();
        }
        Object[] convCols = new Object[] {conv.convert(value)};
        int bucketId = getBucketNumber(convCols, new ObjectInspector[]{constOI}, bucketVersion, numBuckets);
        return Optional.of(ImmutableSet.of(bucketId));
    }

    private static Optional<Set<Integer>> pruneBucketsFromPredicate(
            Expr dorisExpr, String bucketCol, int bucketVersion, int numBuckets) throws DdlException {
        TExprOpcode opcode = dorisExpr.getOpcode();
        switch (opcode) {
            case EQ: {
                // Make sure the col slot is always first
                SlotRef slotRef = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(dorisExpr.getChild(0));
                LiteralExpr literalExpr =
                        HiveMetaStoreClientHelper.convertDorisExprToLiteralExpr(dorisExpr.getChild(1));
                return getPrunedBucketsFromLiteral(slotRef, literalExpr, bucketCol, bucketVersion, numBuckets);
            }
            case FILTER_IN: {
                SlotRef slotRef = HiveMetaStoreClientHelper.convertDorisExprToSlotRef(dorisExpr.getChild(0));
                Optional<Set<Integer>> result = Optional.empty();
                for (int i = 1; i < dorisExpr.getChildren().size(); i++) {
                    LiteralExpr literalExpr =
                            HiveMetaStoreClientHelper.convertDorisExprToLiteralExpr(dorisExpr.getChild(i));
                    Optional<Set<Integer>> childBucket =
                            getPrunedBucketsFromLiteral(slotRef, literalExpr, bucketCol, bucketVersion, numBuckets);
                    if (childBucket.isPresent()) {
                        if (result.isPresent()) {
                            result.get().addAll(childBucket.get());
                        } else {
                            result = Optional.of(new HashSet<>(childBucket.get()));
                        }
                    } else {
                        return Optional.empty();
                    }
                }
                return result;
            }
            default:
                return Optional.empty();
        }
    }

    private static int getBucketNumber(
            Object[] bucketFields, ObjectInspector[] bucketFieldInspectors, int bucketVersion, int numBuckets)
            throws DdlException {
        int hashCode = bucketVersion == 2 ? getBucketHashCodeV2(bucketFields, bucketFieldInspectors)
                : getBucketHashCodeV1(bucketFields, bucketFieldInspectors);
        return (hashCode & Integer.MAX_VALUE) % numBuckets;
    }

    private static int getBucketHashCodeV1(Object[] bucketFields, ObjectInspector[] bucketFieldInspectors)
            throws DdlException {
        int hashCode = 0;
        for (int i = 0; i < bucketFields.length; i++) {
            int fieldHash = hashCodeV1(bucketFields[i], bucketFieldInspectors[i]);
            hashCode = 31 * hashCode + fieldHash;
        }
        return hashCode;
    }

    private static int getBucketHashCodeV2(Object[] bucketFields, ObjectInspector[] bucketFieldInspectors)
            throws DdlException {
        int hashCode = 0;
        ByteBuffer b = ByteBuffer.allocate(8); // To be used with primitive types
        for (int i = 0; i < bucketFields.length; i++) {
            int fieldHash = hashCodeV2(bucketFields[i], bucketFieldInspectors[i], b);
            hashCode = 31 * hashCode + fieldHash;
        }
        return hashCode;
    }

    private static int hashCodeV1(Object o, ObjectInspector objIns) throws DdlException {
        if (o == null) {
            return 0;
        }
        if (objIns.getCategory() == Category.PRIMITIVE) {
            PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objIns);
            switch (poi.getPrimitiveCategory()) {
                case BOOLEAN:
                    return ((BooleanObjectInspector) poi).get(o) ? 1 : 0;
                case BYTE:
                    return ((ByteObjectInspector) poi).get(o);
                case SHORT:
                    return ((ShortObjectInspector) poi).get(o);
                case INT:
                    return ((IntObjectInspector) poi).get(o);
                case LONG: {
                    long a = ((LongObjectInspector) poi).get(o);
                    return (int) ((a >>> 32) ^ a);
                }
                case STRING: {
                    // This hash function returns the same result as String.hashCode() when
                    // all characters are ASCII, while Text.hashCode() always returns a
                    // different result.
                    Text t = ((StringObjectInspector) poi).getPrimitiveWritableObject(o);
                    int r = 0;
                    for (int i = 0; i < t.getLength(); i++) {
                        r = r * 31 + t.getBytes()[i];
                    }
                    return r;
                }
                default:
                    throw new DdlException("Unknown type: " + poi.getPrimitiveCategory());
            }
        }
        throw new DdlException("Unknown type: " + objIns.getTypeName());
    }

    private static int hashCodeV2(Object o, ObjectInspector objIns, ByteBuffer byteBuffer) throws DdlException {
        // Reset the bytebuffer
        byteBuffer.clear();
        if (objIns.getCategory() == Category.PRIMITIVE) {
            PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) objIns);
            switch (poi.getPrimitiveCategory()) {
                case BOOLEAN:
                    return (((BooleanObjectInspector) poi).get(o) ? 1 : 0);
                case BYTE:
                    return ((ByteObjectInspector) poi).get(o);
                case SHORT: {
                    byteBuffer.putShort(((ShortObjectInspector) poi).get(o));
                    return Murmur3.hash32(byteBuffer.array(), 2, 104729);
                }
                case INT: {
                    byteBuffer.putInt(((IntObjectInspector) poi).get(o));
                    return Murmur3.hash32(byteBuffer.array(), 4, 104729);
                }
                case LONG: {
                    byteBuffer.putLong(((LongObjectInspector) poi).get(o));
                    return Murmur3.hash32(byteBuffer.array(), 8, 104729);
                }
                case STRING: {
                    // This hash function returns the same result as String.hashCode() when
                    // all characters are ASCII, while Text.hashCode() always returns a
                    // different result.
                    Text text = ((StringObjectInspector) poi).getPrimitiveWritableObject(o);
                    return Murmur3.hash32(text.getBytes(), text.getLength(), 104729);
                }
                default:
                    throw new DdlException("Unknown type: " + poi.getPrimitiveCategory());
            }
        }
        throw new DdlException("Unknown type: " + objIns.getTypeName());
    }

    private static OptionalInt getBucketNumberFromPath(String name) {
        for (Pattern pattern : BUCKET_PATTERNS) {
            Matcher matcher = pattern.matcher(name);
            if (matcher.matches()) {
                return OptionalInt.of(Integer.parseInt(matcher.group(1)));
            }
        }
        return OptionalInt.empty();
    }
}
