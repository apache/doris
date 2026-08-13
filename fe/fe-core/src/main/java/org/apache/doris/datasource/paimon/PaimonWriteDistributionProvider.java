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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.ExternalWriteDistributionPlan;
import org.apache.doris.datasource.ExternalWriteDistributionProvider;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.PaimonBinaryRowHash;
import org.apache.doris.nereids.trees.expressions.functions.scalar.PaimonFixedBucket;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Plans the native, stateless route supported for Paimon fixed-bucket tables. */
public final class PaimonWriteDistributionProvider
        implements ExternalWriteDistributionProvider<FileStoreTable> {
    private static final String PARTITION_HASH_COLUMN =
            "__doris_write_route_paimon_partition_hash";
    private static final String BUCKET_COLUMN = "__doris_write_route_paimon_bucket";

    @Override
    public ExternalWriteDistributionPlan plan(FileStoreTable table, List<Slot> sinkOutput) {
        if (table.bucketMode() != BucketMode.HASH_FIXED) {
            return ExternalWriteDistributionPlan.singleWriter(
                    "Paimon bucket mode requires a stateful or unsupported route: "
                            + table.bucketMode());
        }

        CoreOptions options = CoreOptions.fromMap(table.options());
        if (options.bucketFunctionType() != CoreOptions.BucketFunctionType.DEFAULT) {
            return ExternalWriteDistributionPlan.singleWriter(
                    "Paimon custom bucket function is not supported by native routing");
        }

        TableSchema schema = table.schema();
        if (schema.numBuckets() <= 0 || schema.bucketKeys().isEmpty()) {
            return ExternalWriteDistributionPlan.singleWriter(
                    "Paimon fixed-bucket metadata is incomplete");
        }

        Map<String, Slot> outputByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Slot slot : sinkOutput) {
            if (outputByName.put(slot.getName(), slot) != null) {
                return ExternalWriteDistributionPlan.singleWriter(
                        "duplicate Paimon sink output column: " + slot.getName());
            }
        }
        Map<String, DataField> fieldsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (DataField field : schema.fields()) {
            fieldsByName.put(field.name(), field);
        }

        List<Expression> partitionFields = projectFields(
                schema.partitionKeys(), outputByName, fieldsByName);
        if (partitionFields == null) {
            return ExternalWriteDistributionPlan.singleWriter(
                    "Paimon partition fields are missing or unsupported by native routing");
        }
        List<Expression> bucketFields = projectFields(
                schema.bucketKeys(), outputByName, fieldsByName);
        if (bucketFields == null || bucketFields.isEmpty()) {
            return ExternalWriteDistributionPlan.singleWriter(
                    "Paimon bucket fields are missing or unsupported by native routing");
        }

        ImmutableList.Builder<NamedExpression> routes = ImmutableList.builder();
        if (!partitionFields.isEmpty()) {
            routes.add(new Alias(
                    new PaimonBinaryRowHash(partitionFields), PARTITION_HASH_COLUMN));
        }
        Alias bucketRoute = new Alias(
                new PaimonFixedBucket(schema.numBuckets(), bucketFields), BUCKET_COLUMN);
        routes.add(bucketRoute);
        return ExternalWriteDistributionPlan.statelessHash(
                routes.build(), ImmutableMap.of(bucketRoute.getExprId(), (long) schema.numBuckets()));
    }

    private List<Expression> projectFields(List<String> names, Map<String, Slot> outputByName,
            Map<String, DataField> fieldsByName) {
        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (String name : names) {
            Slot slot = outputByName.get(name);
            DataField field = fieldsByName.get(name);
            if (slot == null || field == null || !supports(field.type().getTypeRoot(), slot.getDataType())) {
                return null;
            }
            result.add(slot);
        }
        return result.build();
    }

    private boolean supports(DataTypeRoot paimonType, DataType dorisType) {
        switch (paimonType) {
            case BOOLEAN:
                return dorisType.isBooleanType();
            case TINYINT:
                return dorisType.isTinyIntType();
            case SMALLINT:
                return dorisType.isSmallIntType();
            case INTEGER:
                return dorisType.isIntegerType();
            case BIGINT:
                return dorisType.isBigIntType();
            case FLOAT:
                return dorisType.isFloatType();
            case DOUBLE:
                return dorisType.isDoubleType();
            case CHAR:
            case VARCHAR:
                return dorisType.isStringLikeType();
            case BINARY:
            case VARBINARY:
                return dorisType.isStringLikeType() || dorisType.isVarBinaryType();
            default:
                return false;
        }
    }
}
