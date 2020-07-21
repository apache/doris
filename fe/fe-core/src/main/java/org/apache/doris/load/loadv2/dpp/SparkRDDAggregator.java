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
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.BitmapValue;
import org.apache.doris.load.loadv2.Hll;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
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

    // TODO(wb) support more datatype:decimal,date,datetime
    public static SparkRDDAggregator buildAggregator(EtlJobConfig.EtlColumn column) throws UserException {
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
                        return new NumberMaxAggregator();
                    case "char":
                    case "varchar":
                        return new StringMaxAggregator();
                    case "largeint":
                        return new LargeIntMaxAggregator();
                    default:
                        throw new UserException(String.format("unsupported max aggregator for column type:%s", columnType));
                }
            case "min":
                switch (columnType) {
                    case "tinyint":
                    case "smallint":
                    case "int":
                    case "bigint":
                    case "float":
                    case "double":
                        return new NumberMinAggregator();
                    case "char":
                    case "varchar":
                        return new StringMinAggregator();
                    case "largeint":
                        return new LargeIntMinAggregator();
                    default:
                        throw new UserException(String.format("unsupported min aggregator for column type:%s", columnType));
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
                    default:
                        throw new UserException(String.format("unsupported sum aggregator for column type:%s", columnType));
                }
            case "replace_if_not_null":
                return new ReplaceIfNotNullAggregator();
            case "replace":
                return new ReplaceAggregator();
            default:
                throw new UserException(String.format("unsupported aggregate type %s", aggType));
        }
    }

}

class EncodeDuplicateTableFunction extends EncodeAggregateTableFunction {

    private int valueLen;

    public EncodeDuplicateTableFunction(int keyLen, int valueLen) {
        super(keyLen);
        this.valueLen = valueLen;
    }

    @Override
    public Tuple2<List<Object>, Object[]> call(Row row) throws Exception {
        List<Object> keys = new ArrayList(keyLen);
        Object[] values = new Object[valueLen];

        for (int i = 0; i < keyLen; i++) {
            keys.add(row.get(i));
        }

        for (int i = keyLen; i < row.length(); i++) {
            values[i - keyLen] = row.get(i);
        }

        return new Tuple2<>(keys, values);
    }
}

class EncodeAggregateTableFunction implements PairFunction<Row, List<Object>, Object[]> {

    private SparkRDDAggregator[] valueAggregators;
    // include bucket id
    protected int keyLen;

    public EncodeAggregateTableFunction(int keyLen) {
        this.keyLen = keyLen;
    }

    public EncodeAggregateTableFunction(SparkRDDAggregator[] valueAggregators, int keyLen) {
        this.valueAggregators = valueAggregators;
        this.keyLen = keyLen;
    }

    // TODO(wb): use a custom class as key to instead of List to save space
    @Override
    public Tuple2<List<Object>, Object[]> call(Row row) throws Exception {
        List<Object> keys = new ArrayList(keyLen);
        Object[] values = new Object[valueAggregators.length];

        for (int i = 0; i < keyLen; i++) {
            keys.add(row.get(i));
        }

        for (int i = keyLen; i < row.size(); i++) {
            int valueIdx = i - keyLen;
            values[valueIdx] = valueAggregators[valueIdx].init(row.get(i));
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
            } else {
                bitmapValue.add(value == null ? 0l : Long.valueOf(value.toString()));
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
            } else {
                hll.updateWithHash(value == null ? 0 : value);
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
        // TODO(wb) check overflow of long type
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
        Integer ret = dst + src;
        if  (ret > Short.MAX_VALUE || ret < Short.MIN_VALUE) {
            throw new RuntimeException("short column sum size exceeds Short.MAX_VALUE or Short.MIN_VALUE");
        }
        return Short.valueOf(ret.toString());
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
        if  (ret > Integer.MAX_VALUE || ret < Integer.MIN_VALUE) {
            throw new RuntimeException("int column sum size exceeds Integer.MAX_VALUE or Integer.MIN_VALUE");
        }
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
        Integer ret = dst + src;
        if  (ret > Byte.MAX_VALUE || ret < Byte.MIN_VALUE) {
            throw new RuntimeException("byte column sum size exceeds Byte.MAX_VALUE or Byte.MIN_VALUE");
        }
        return Byte.valueOf(ret.toString());
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

// TODO(wb) add bound check for float/double
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