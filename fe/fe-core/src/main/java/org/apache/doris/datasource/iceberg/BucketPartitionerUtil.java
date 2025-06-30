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

package org.apache.doris.datasource.iceberg;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.transforms.PartitionSpecVisitor;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

final class BucketPartitionerUtil {

    private BucketPartitionerUtil() {}

    public static List<Tuple2<Integer, Integer>> getBucketFields(PartitionSpec spec) {
        return PartitionSpecVisitor.visit(spec, new BucketPartitionSpecVisitor()).stream()
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    private static class BucketPartitionSpecVisitor implements PartitionSpecVisitor<Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> identity(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Tuple2<Integer, Integer> bucket(
                int fieldId, String sourceName, int sourceId, int numBuckets) {
            return new Tuple2<>(fieldId, numBuckets);
        }

        @Override
        public Tuple2<Integer, Integer> truncate(
                int fieldId, String sourceName, int sourceId, int width) {
            return null;
        }

        @Override
        public Tuple2<Integer, Integer> year(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Tuple2<Integer, Integer> month(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Tuple2<Integer, Integer> day(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Tuple2<Integer, Integer> hour(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Tuple2<Integer, Integer> alwaysNull(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Tuple2<Integer, Integer> unknown(
                int fieldId, String sourceName, int sourceId, String transform) {
            return null;
        }
    }
}
