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

package org.apache.doris.load.loadv2.dpp;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.doris.common.SparkDppException;
import org.apache.doris.common.io.BitmapValue;
import org.apache.doris.common.io.Hll;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

// contains all class about spark aggregate

public abstract class SparkRDDAggregator<T> implements Serializable {

    T init(Object value) {
        return (T) value;
    }

    abstract T update(T v1, T v2);

    Object finalize(Object value) {
        return value;
    };

    public static SparkRDDAggregator buildAggregator(EtlJobConfig.EtlColumn column) throws SparkDppException {
        String aggType = StringUtils.lowerCase(column.aggregationType);
        String columnType = StringUtils.lowerCase(column.columnType);
        switch (aggType) {
            case "bitmap_union" :
                return new BitmapUnionAggregator();
            case "hll_union" :
                return new HllUnionAggregator();
            case "max":
                switch (columnType) {
                    case "tinyint":
                    case "smallint":
                    case "int":
                    case "bigint":
                    case "float":
                    case "double":
                    case "decimalv2":
                    case "date":
                    case "datetime":
                        return new NumberMaxAggregator();
                    case "char":
                    case "varchar":
                        return new StringMaxAggregator();
                    case "largeint":
                        return new LargeIntMaxAggregator();
                    default:
                        throw new SparkDppException(String.format("unsupported max aggregator for column type:%s", columnType));
                }
            case "min":
                switch (columnType) {
                    case "tinyint":
                    case "smallint":
                    case "int":
                    case "bigint":
                    case "float":
                    case "double":
                    case "decimalv2":
                    case "date":
                    case "datetime":
                        return new NumberMinAggregator();
                    case "char":
                    case "varchar":
                        return new StringMinAggregator();
                    case "largeint":
                        return new LargeIntMinAggregator();
                    default:
                        throw new SparkDppException(String.format("unsupported min aggregator for column type:%s", columnType));
                }
            case "sum":
                switch (columnType) {
                    case "tinyint":
                        return new ByteSumAggregator();
                    case "smallint":
                        return new ShortSumAggregator();
                    case "int":
                        return new IntSumAggregator();
                    case "bigint":
                        return new LongSumAggregator();
                    case "float":
                        return new FloatSumAggregator();
                    case "double":
                        return new DoubleSumAggregator();
                    case "largeint":
                        return new LargeIntSumAggregator();
                    case "decimalv2":
                        return new BigDecimalSumAggregator();
                    default:
                        throw new SparkDppException(String.format("unsupported sum aggregator for column type:%s", columnType));
                }
            case "replace_if_not_null":
                return new ReplaceIfNotNullAggregator();
            case "replace":
                return new ReplaceAggregator();
            default:
                throw new SparkDppException(String.format("unsupported aggregate type %s", aggType));
        }
    }

}

// just used for duplicate table, default logic is enough
class DefaultSparkRDDAggregator extends SparkRDDAggregator {

    @Override
    Object update(Object v1, Object v2) {
        return null;
    }
}

// just encode value column,used for base rollup
class EncodeBaseAggregateTableFunction implements PairFunction<Tuple2<List<Object>, Object[]>, List<Object>, Object[]> {

    private SparkRDDAggregator[] valueAggregators;

    public EncodeBaseAggregateTableFunction(SparkRDDAggregator[] valueAggregators) {
        this.valueAggregators = valueAggregators;
    }


    @Override
    public Tuple2<List<Object>, Object[]> call(Tuple2<List<Object>, Object[]> srcPair) throws Exception {
        for (int i = 0; i < srcPair._2().length; i++) {
            srcPair._2()[i] = valueAggregators[i].init(srcPair._2()[i]);
        }
        return srcPair;
    }
}

// just map column from parent rollup index to child rollup index,used for child rollup
class EncodeRollupAggregateTableFunction implements PairFunction<Tuple2<List<Object>, Object[]>, List<Object>, Object[]> {

    Pair<Integer[], Integer[]> columnIndexInParentRollup;

    public EncodeRollupAggregateTableFunction(Pair<Integer[], Integer[]> columnIndexInParentRollup) {
        this.columnIndexInParentRollup = columnIndexInParentRollup;
    }

    @Override
    public Tuple2<List<Object>, Object[]> call(Tuple2<List<Object>, Object[]> parentRollupKeyValuePair) throws Exception {
        Integer[] keyColumnIndexMap = columnIndexInParentRollup.getKey();
        Integer[] valueColumnIndexMap = columnIndexInParentRollup.getValue();

        List<Object> keys = new ArrayList();
        Object[] values = new Object[valueColumnIndexMap.length];

        // deal bucket_id column
        keys.add(parentRollupKeyValuePair._1().get(0));
        for (int i = 0; i < keyColumnIndexMap.length; i++) {
            keys.add(parentRollupKeyValuePair._1().get(keyColumnIndexMap[i] + 1));
        }

        for (int i = 0; i < valueColumnIndexMap.length; i++) {
            values[i] = parentRollupKeyValuePair._2()[valueColumnIndexMap[i]];
        }
        return new Tuple2<>(keys, values);
    }
}

class AggregateReduceFunction implements Function2<Object[], Object[], Object[]> {

    private SparkRDDAggregator[] valueAggregators;

    public AggregateReduceFunction(SparkRDDAggregator[] sparkDppAggregators) {
        this.valueAggregators = sparkDppAggregators;
    }

    @Override
    public Object[] call(Object[] v1, Object[] v2) throws Exception {
        Object[] result = new Object[valueAggregators.length];
        for (int i = 0; i < v1.length; i++) {
            result[i] = valueAggregators[i].update(v1[i], v2[i]);
        }
        return result;
    }
}

class ReplaceAggregator extends SparkRDDAggregator<Object> {

    @Override
    Object update(Object dst, Object src) {
        return src;
    }
}

class ReplaceIfNotNullAggregator extends SparkRDDAggregator<Object> {

    @Override
    Object update(Object dst, Object src) {
        return src == null ? dst : src;
    }
}

class BitmapUnionAggregator extends SparkRDDAggregator<BitmapValue> {

    @Override
    BitmapValue init(Object value) {
        try {
            BitmapValue bitmapValue = new BitmapValue();
            if (value instanceof byte[]) {
                bitmapValue.deserialize(new DataInputStream(new ByteArrayInputStream((byte[]) value)));
            } else if (value != null){
                bitmapValue.add(Long.valueOf(value.toString()));
            }
            return bitmapValue;
        } catch (Exception e) {
            throw new RuntimeException("build bitmap value failed", e);
        }
    }

    @Override
    BitmapValue update(BitmapValue v1, BitmapValue v2) {
        BitmapValue newBitmapValue = new BitmapValue();
        if (v1 != null) {
            newBitmapValue.or(v1);
        }
        if (v2 != null) {
            newBitmapValue.or(v2);
        }
        return newBitmapValue;
    }

    @Override
    byte[] finalize(Object value) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(bos);
            ((BitmapValue)value).serialize(outputStream);
            return bos.toByteArray();
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new RuntimeException(ioException);
        }
    }

}

class HllUnionAggregator extends SparkRDDAggregator<Hll> {

    @Override
    Hll init(Object value) {
        try {
            Hll hll = new Hll();
            if (value instanceof byte[]) {
                hll.deserialize(new DataInputStream(new ByteArrayInputStream((byte[]) value)));
            } else if (value != null){
                hll.updateWithHash(value);
            }
            return hll;
        } catch (Exception e) {
            throw new RuntimeException("build hll value failed", e);
        }
    }

    @Override
    Hll update(Hll v1, Hll v2) {
        Hll newHll = new Hll();
        if (v1 != null) {
            newHll.merge(v1);
        }
        if (v2 != null) {
            newHll.merge(v2);
        }
        return newHll;
    }

    @Override
    byte[] finalize(Object value) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(bos);
            ((Hll)value).serialize(outputStream);
            return bos.toByteArray();
        } catch (IOException ioException) {
            ioException.printStackTrace();
            throw new RuntimeException(ioException);
        }
    }

}

class LargeIntMaxAggregator extends SparkRDDAggregator<BigInteger> {

    BigInteger init(Object value) {
        if (value == null) {
            return null;
        }
        return new BigInteger(value.toString());
    }

    @Override
    BigInteger update(BigInteger dst, BigInteger src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst.compareTo(src) > 0 ? dst : src;
    }

    @Override
    String finalize(Object value) {
        BigInteger bigInteger = (BigInteger) value;
        return bigInteger.toString();
    }
}

class LargeIntMinAggregator extends LargeIntMaxAggregator {

    @Override
    BigInteger update(BigInteger dst, BigInteger src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst.compareTo(src) < 0 ? dst : src;
    }
}

class LargeIntSumAggregator extends LargeIntMaxAggregator {

    @Override
    BigInteger update(BigInteger dst, BigInteger src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst.add(src);
    }
}


class NumberMaxAggregator extends SparkRDDAggregator {

    @Override
    Object update(Object dst, Object src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return ((Comparable)dst).compareTo(src) > 0 ? dst : src;
    }
}


class NumberMinAggregator extends SparkRDDAggregator {

    @Override
    Object update(Object dst, Object src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return ((Comparable)dst).compareTo(src) < 0 ? dst : src;
    }
}

class LongSumAggregator extends SparkRDDAggregator<Long> {

    @Override
    Long update(Long dst, Long src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst + src;
    }
}

class ShortSumAggregator extends SparkRDDAggregator<Short> {

    @Override
    Short update(Short dst, Short src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        int ret = dst + src;
        // here may overflow, just keep the same logic with be
        return (short)ret;
    }
}

class IntSumAggregator extends SparkRDDAggregator<Integer> {

    @Override
    Integer update(Integer dst, Integer src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        long ret = Long.sum(dst, src);
        // here may overflow, just keep the same logic with be
        return (int) ret;
    }
}

class ByteSumAggregator extends SparkRDDAggregator<Byte> {

    @Override
    Byte update(Byte dst, Byte src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        int ret = dst + src;
        // here may overflow, just keep the same logic with be
        return (byte)ret;
    }
}

class DoubleSumAggregator extends SparkRDDAggregator<Double> {

    @Override
    strictfp Double update(Double dst, Double src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst + src;
    }
}

class FloatSumAggregator extends SparkRDDAggregator<Float> {

    @Override
    strictfp Float update(Float dst, Float src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst + src;
    }
}

class StringMaxAggregator extends SparkRDDAggregator<String> {

    @Override
    String update(String dst, String src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst.compareTo(src) > 0 ? dst : src;
    }
}

class StringMinAggregator extends SparkRDDAggregator<String> {

    @Override
    String update(String dst, String src) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return dst.compareTo(src) < 0 ? dst : src;
    }
}

class BigDecimalSumAggregator extends SparkRDDAggregator<BigDecimal> {


    @Override
    BigDecimal update(BigDecimal src, BigDecimal dst) {
        if (src == null) {
            return dst;
        }
        if (dst == null) {
            return src;
        }
        return src.add(dst);
    }
}


class BucketComparator implements Comparator<List<Object>>, Serializable {

    @Override
    public int compare(List<Object> keyArray1, List<Object> keyArray2) {
        int cmp = 0;

        for (int i = 0; i < keyArray1.size(); i++) {
            Object key1 = keyArray1.get(i);
            Object key2 = keyArray2.get(i);
            if (key1 == key2) {
                continue;
            }
            if (key1 == null || key2 == null) {
                return key1 == null ? -1 : 1;
            }
            if (key1 instanceof Comparable && key2 instanceof Comparable) {
                cmp = ((Comparable) key1).compareTo(key2);
            } else {
                throw new RuntimeException(String.format("uncomparable column type %s", key1.getClass().toString()));
            }
            if (cmp != 0) {
                return cmp;
            }
        }

        return cmp;
    }
}

class BucketPartitioner extends Partitioner {

    private Map<String, Integer> bucketKeyMap;

    public BucketPartitioner(Map<String, Integer> bucketKeyMap) {
        this.bucketKeyMap = bucketKeyMap;
    }

    @Override
    public int numPartitions() {
        return bucketKeyMap.size();
    }

    @Override
    public int getPartition(Object key) {
        List<Object> rddKey = (List<Object>) key;
        return bucketKeyMap.get(String.valueOf(rddKey.get(0)));
    }
}