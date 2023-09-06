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

import org.apache.doris.common.SparkDppException;

import org.apache.spark.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.List;

public abstract class DorisPartitioner extends Partitioner implements Comparator<List<Object>>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadJobV2.class);
    protected static final String UNPARTITIONED_TYPE = "UNPARTITIONED";

    // partition keys will be parsed into double from json
    // so need to convert it to partition columns' type
    public static Object convertPartitionKey(Object srcValue, Class dstClass) throws SparkDppException {
        if (dstClass.equals(Float.class) || dstClass.equals(Double.class)) {
            return null;
        }
        if (srcValue instanceof Double) {
            if (dstClass.equals(Short.class)) {
                return ((Double) srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return ((Double) srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
                return ((Double) srcValue).longValue();
            } else if (dstClass.equals(BigInteger.class)) {
                // TODO(wb) gson will cast origin value to double by default
                // when the partition column is largeint, this will cause error data
                // need fix it thoroughly
                return new BigInteger(srcValue.toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDate((int) srcValueDouble);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDatetime((long) srcValueDouble);
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else {
            LOG.error("unsupported partition key:" + srcValue);
            throw new SparkDppException("unsupported partition key:" + srcValue);
        }
    }

    public static java.sql.Timestamp convertToJavaDatetime(long src) {
        String dateTimeStr = Long.valueOf(src).toString();
        if (dateTimeStr.length() != 14) {
            throw new RuntimeException("invalid input date format for SparkDpp");
        }

        String year = dateTimeStr.substring(0, 4);
        String month = dateTimeStr.substring(4, 6);
        String day = dateTimeStr.substring(6, 8);
        String hour = dateTimeStr.substring(8, 10);
        String min = dateTimeStr.substring(10, 12);
        String sec = dateTimeStr.substring(12, 14);

        return java.sql.Timestamp.valueOf(String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, min, sec));
    }

    public static java.sql.Date convertToJavaDate(int originDate) {
        int day = originDate & 0x1f;
        originDate >>= 5;
        int month = originDate & 0x0f;
        originDate >>= 4;
        int year = originDate;
        return java.sql.Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
    }

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
                throw new RuntimeException(String.format("incomparable column type %s", key1.getClass().toString()));
            }
            if (cmp != 0) {
                return cmp;
            }
        }

        return cmp;
    }
}
