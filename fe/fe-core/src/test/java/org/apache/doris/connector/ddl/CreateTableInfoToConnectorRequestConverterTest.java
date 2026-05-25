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

package org.apache.doris.connector.ddl;

import org.apache.doris.catalog.PartitionType;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ddl.ConnectorBucketSpec;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorPartitionField;
import org.apache.doris.connector.api.ddl.ConnectorPartitionSpec;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DistributionDescriptor;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Covers each branch of {@link CreateTableInfoToConnectorRequestConverter}:
 * the four partition styles (IDENTITY, TRANSFORM, LIST, RANGE) and both
 * bucket flavors (hash, random), plus the no-partition / no-distribution
 * fall-throughs.
 *
 * <p>{@link CreateTableInfo} is mocked because its full constructor pulls in
 * heavy nereids analyzer state; the converter only reads a handful of
 * getters from it, all of which are easy to stub.
 */
public class CreateTableInfoToConnectorRequestConverterTest {

    @Test
    public void columnsAndScalarFieldsArePassedThrough() {
        ColumnDefinition idCol = new ColumnDefinition(
                "id", IntegerType.INSTANCE, false, "primary key");
        ColumnDefinition nameCol = new ColumnDefinition(
                "name", StringType.INSTANCE, true, "display name");
        CreateTableInfo info = stubInfo(
                "orders",
                Arrays.asList(idCol, nameCol),
                null,
                null,
                "an orders table",
                ImmutableMap.of("k", "v"),
                true,
                true);

        ConnectorCreateTableRequest req = CreateTableInfoToConnectorRequestConverter
                .convert(info, "sales");

        Assertions.assertEquals("sales", req.getDbName());
        Assertions.assertEquals("orders", req.getTableName());
        Assertions.assertEquals("an orders table", req.getComment());
        Assertions.assertEquals(ImmutableMap.of("k", "v"), req.getProperties());
        Assertions.assertTrue(req.isIfNotExists());
        Assertions.assertTrue(req.isExternal());

        Assertions.assertEquals(2, req.getColumns().size());
        ConnectorColumn col0 = req.getColumns().get(0);
        Assertions.assertEquals("id", col0.getName());
        Assertions.assertFalse(col0.isNullable());
        Assertions.assertEquals("primary key", col0.getComment());
        ConnectorColumn col1 = req.getColumns().get(1);
        Assertions.assertEquals("name", col1.getName());
        Assertions.assertTrue(col1.isNullable());

        // No partition / distribution in this fixture.
        Assertions.assertNull(req.getPartitionSpec());
        Assertions.assertNull(req.getBucketSpec());
    }

    @Test
    public void identityPartitionStyle() {
        // PARTITIONED BY (dt) on a Hive-style external table.
        PartitionTableInfo partition = new PartitionTableInfo(
                false,
                PartitionType.UNPARTITIONED.name(),
                null,
                ImmutableList.of(new UnboundSlot("dt")));
        ConnectorPartitionSpec spec = convertWithPartition(partition).getPartitionSpec();

        Assertions.assertNotNull(spec);
        Assertions.assertEquals(ConnectorPartitionSpec.Style.IDENTITY, spec.getStyle());
        Assertions.assertEquals(1, spec.getFields().size());
        ConnectorPartitionField field = spec.getFields().get(0);
        Assertions.assertEquals("dt", field.getColumnName());
        Assertions.assertEquals("identity", field.getTransform());
        Assertions.assertTrue(field.getTransformArgs().isEmpty());
        Assertions.assertTrue(spec.getInitialValues().isEmpty());
    }

    @Test
    public void transformPartitionStyleWithIcebergStyleFunctions() {
        // PARTITIONED BY (bucket(16, id), year(d)) — Iceberg style.
        Expression bucket = new UnboundFunction("bucket",
                Arrays.asList(new UnboundSlot("id"), new IntegerLiteral(16)));
        Expression year = new UnboundFunction("YEAR",
                Collections.singletonList(new UnboundSlot("d")));
        PartitionTableInfo partition = new PartitionTableInfo(
                false,
                PartitionType.UNPARTITIONED.name(),
                null,
                ImmutableList.of(bucket, year));

        ConnectorPartitionSpec spec = convertWithPartition(partition).getPartitionSpec();
        Assertions.assertNotNull(spec);
        Assertions.assertEquals(ConnectorPartitionSpec.Style.TRANSFORM, spec.getStyle());

        Assertions.assertEquals(2, spec.getFields().size());
        ConnectorPartitionField bucketField = spec.getFields().get(0);
        Assertions.assertEquals("id", bucketField.getColumnName());
        Assertions.assertEquals("bucket", bucketField.getTransform());
        Assertions.assertEquals(Collections.singletonList(16), bucketField.getTransformArgs());

        ConnectorPartitionField yearField = spec.getFields().get(1);
        Assertions.assertEquals("d", yearField.getColumnName());
        // transform name is lower-cased even though the source was uppercase.
        Assertions.assertEquals("year", yearField.getTransform());
        Assertions.assertTrue(yearField.getTransformArgs().isEmpty());
    }

    @Test
    public void listPartitionStyle() {
        // PARTITION BY LIST (region) — Doris native list partitioning.
        PartitionTableInfo partition = new PartitionTableInfo(
                false,
                PartitionType.LIST.name(),
                null,
                ImmutableList.of(new UnboundSlot("region")));

        ConnectorPartitionSpec spec = convertWithPartition(partition).getPartitionSpec();
        Assertions.assertNotNull(spec);
        Assertions.assertEquals(ConnectorPartitionSpec.Style.LIST, spec.getStyle());
        Assertions.assertEquals(1, spec.getFields().size());
        Assertions.assertEquals("region", spec.getFields().get(0).getColumnName());
        // initialValues lowering is deferred — see converter inline comment.
        Assertions.assertTrue(spec.getInitialValues().isEmpty());
    }

    @Test
    public void rangePartitionStyle() {
        // PARTITION BY RANGE (dt) — Doris native range partitioning.
        PartitionTableInfo partition = new PartitionTableInfo(
                false,
                PartitionType.RANGE.name(),
                null,
                ImmutableList.of(new UnboundSlot("dt")));

        ConnectorPartitionSpec spec = convertWithPartition(partition).getPartitionSpec();
        Assertions.assertNotNull(spec);
        Assertions.assertEquals(ConnectorPartitionSpec.Style.RANGE, spec.getStyle());
        Assertions.assertEquals(1, spec.getFields().size());
        Assertions.assertEquals("dt", spec.getFields().get(0).getColumnName());
        Assertions.assertTrue(spec.getInitialValues().isEmpty());
    }

    @Test
    public void hashDistributionMapsToDorisDefaultAlgorithm() {
        DistributionDescriptor dd = new DistributionDescriptor(
                true, false, 4, Arrays.asList("id"));
        ConnectorBucketSpec bucket = convertWithDistribution(dd).getBucketSpec();

        Assertions.assertNotNull(bucket);
        Assertions.assertEquals(Arrays.asList("id"), bucket.getColumns());
        Assertions.assertEquals(4, bucket.getNumBuckets());
        Assertions.assertEquals("doris_default", bucket.getAlgorithm());
    }

    @Test
    public void randomDistributionMapsToDorisRandomAlgorithm() {
        DistributionDescriptor dd = new DistributionDescriptor(
                false, false, 8, Collections.emptyList());
        ConnectorBucketSpec bucket = convertWithDistribution(dd).getBucketSpec();

        Assertions.assertNotNull(bucket);
        Assertions.assertEquals(Collections.emptyList(), bucket.getColumns());
        Assertions.assertEquals(8, bucket.getNumBuckets());
        Assertions.assertEquals("doris_random", bucket.getAlgorithm());
    }

    // ──────────────────── helpers ────────────────────

    private static ConnectorCreateTableRequest convertWithPartition(
            PartitionTableInfo partition) {
        return CreateTableInfoToConnectorRequestConverter.convert(
                stubInfo("t",
                        Collections.singletonList(new ColumnDefinition(
                                "id", IntegerType.INSTANCE, true)),
                        partition,
                        null,
                        "",
                        Collections.emptyMap(),
                        false,
                        false),
                "db");
    }

    private static ConnectorCreateTableRequest convertWithDistribution(
            DistributionDescriptor distribution) {
        return CreateTableInfoToConnectorRequestConverter.convert(
                stubInfo("t",
                        Collections.singletonList(new ColumnDefinition(
                                "id", IntegerType.INSTANCE, true)),
                        null,
                        distribution,
                        "",
                        Collections.emptyMap(),
                        false,
                        false),
                "db");
    }

    /**
     * Builds a mock {@link CreateTableInfo} answering only the getters that
     * the converter actually reads. Saves the test from threading 18 args
     * through the real ctor (which also calls {@code PropertyAnalyzer}).
     */
    private static CreateTableInfo stubInfo(String tableName,
            List<ColumnDefinition> columns,
            PartitionTableInfo partition,
            DistributionDescriptor distribution,
            String comment,
            java.util.Map<String, String> properties,
            boolean ifNotExists,
            boolean external) {
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getTableName()).thenReturn(tableName);
        Mockito.when(info.getColumnDefinitions()).thenReturn(columns);
        Mockito.when(info.getPartitionTableInfo()).thenReturn(partition);
        Mockito.when(info.getDistribution()).thenReturn(distribution);
        Mockito.when(info.getComment()).thenReturn(comment);
        Mockito.when(info.getProperties()).thenReturn(properties);
        Mockito.when(info.isIfNotExists()).thenReturn(ifNotExists);
        Mockito.when(info.isExternal()).thenReturn(external);
        return info;
    }
}
