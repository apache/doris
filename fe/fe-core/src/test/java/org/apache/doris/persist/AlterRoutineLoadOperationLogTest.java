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

package org.apache.doris.persist;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimeV2Literal;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class AlterRoutineLoadOperationLogTest {
    private static final String A8928245_LEGACY_LOG =
            "/upgrade/routine-load/a8928245/alter-routine-load-log.b64";

    @Test
    public void testSerializeAlterRoutineLoadOperationLog() throws IOException, UserException {
        long jobId = 1000;
        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY, "5");

        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put(KafkaConfiguration.KAFKA_PARTITIONS.getName(), "0, 1");
        dataSourceProperties.put(KafkaConfiguration.KAFKA_OFFSETS.getName(), "10000, 20000");
        dataSourceProperties.put("property.group.id", "mygroup");
        KafkaDataSourceProperties routineLoadDataSourceProperties = new KafkaDataSourceProperties(
                dataSourceProperties);
        routineLoadDataSourceProperties.setAlter(true);
        routineLoadDataSourceProperties.setTimezone(TimeUtils.DEFAULT_TIME_ZONE);
        routineLoadDataSourceProperties.analyze();

        Separator columnSeparator = new Separator(",", "\\x2c");
        Separator lineDelimiter = new Separator("\n", "\\n");
        List<ImportColumnDesc> columns = Lists.newArrayList(
                new ImportColumnDesc("source_col"),
                new ImportColumnDesc("mapped_col", new TimeV2Literal(12, 34, 56, 123456, 6, true)));
        Expr precedingFilter = new MatchPredicate(MatchPredicate.Operator.MATCH_ANY,
                namedSlot("content"), new StringLiteral("hello world"), Type.BOOLEAN,
                NullableMode.DEPEND_ON_ARGUMENT, null, false, "english");
        Expr where = new BinaryPredicate(BinaryPredicate.Operator.GT,
                namedSlot("a`b"), new IntLiteral(10L));
        PartitionNamesInfo partitions = new PartitionNamesInfo(true, Lists.newArrayList("p1", "p2"));
        BinaryPredicate deleteCondition = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new IntLiteral(1L), new IntLiteral(1L));
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, lineDelimiter, columns,
                precedingFilter, where, partitions, deleteCondition, LoadTask.MergeType.MERGE, "sequence_col");
        String expectedColumnSql = exprToSql(columns.get(1).getExpr());
        String expectedPrecedingSql = exprToSql(precedingFilter);
        String expectedWhereSql = exprToSql(where);
        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(jobId,
                jobProperties, routineLoadDataSourceProperties, routineLoadDesc);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            log.write(out);
        }

        AlterRoutineLoadJobOperationLog log2;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            log2 = AlterRoutineLoadJobOperationLog.read(in);
        }
        Assert.assertEquals(1, log2.getJobProperties().size());
        Assert.assertEquals("5", log2.getJobProperties().get(CreateRoutineLoadInfo.DESIRED_CONCURRENT_NUMBER_PROPERTY));
        KafkaDataSourceProperties kafkaDataSourceProperties = (KafkaDataSourceProperties) log2.getDataSourceProperties();
        Assert.assertEquals(null, kafkaDataSourceProperties.getBrokerList());
        Assert.assertEquals(null, kafkaDataSourceProperties.getTopic());
        Assert.assertEquals(1, kafkaDataSourceProperties.getCustomKafkaProperties().size());
        Assert.assertEquals("mygroup", kafkaDataSourceProperties.getCustomKafkaProperties().get("group.id"));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(0),
                kafkaDataSourceProperties.getKafkaPartitionOffsets().get(0));
        Assert.assertEquals(routineLoadDataSourceProperties.getKafkaPartitionOffsets().get(1),
                kafkaDataSourceProperties.getKafkaPartitionOffsets().get(1));
        RoutineLoadDesc restoredDesc = log2.getRoutineLoadDesc();
        Assert.assertEquals(",", restoredDesc.getColumnSeparator().getSeparator());
        Assert.assertEquals("\\x2c", restoredDesc.getColumnSeparator().getOriSeparator());
        Assert.assertEquals("\n", restoredDesc.getLineDelimiter().getSeparator());
        Assert.assertEquals("\\n", restoredDesc.getLineDelimiter().getOriSeparator());
        Assert.assertEquals(2, restoredDesc.getColumnsInfo().size());
        Assert.assertEquals("source_col", restoredDesc.getColumnsInfo().get(0).getColumnName());
        Assert.assertEquals("mapped_col", restoredDesc.getColumnsInfo().get(1).getColumnName());
        Assert.assertNotNull(restoredDesc.getColumnsInfo().get(1).getExpr());
        Assert.assertNotNull(restoredDesc.getPrecedingFilter());
        Assert.assertNotNull(restoredDesc.getFilter());
        Assert.assertEquals(expectedColumnSql, exprToSql(restoredDesc.getColumnsInfo().get(1).getExpr()));
        Assert.assertEquals(expectedPrecedingSql, exprToSql(restoredDesc.getPrecedingFilter()));
        Assert.assertEquals(expectedWhereSql, exprToSql(restoredDesc.getFilter()));
        Assert.assertTrue(restoredDesc.getPartitionNamesInfo().isTemp());
        Assert.assertEquals(Lists.newArrayList("p1", "p2"),
                restoredDesc.getPartitionNamesInfo().getPartitionNames());
        Assert.assertNotNull(restoredDesc.getDeleteCondition());
        Assert.assertEquals(LoadTask.MergeType.MERGE, restoredDesc.getMergeType());
        Assert.assertEquals("sequence_col", restoredDesc.getSequenceColName());
        Assert.assertEquals(GsonUtils.GSON.toJson(routineLoadDesc), GsonUtils.GSON.toJson(restoredDesc));
    }

    @Test
    public void testDeserializeLegacyLogWithoutRoutineLoadDesc() throws IOException {
        byte[] bytes = loadBase64Fixture(A8928245_LEGACY_LOG);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            AlterRoutineLoadJobOperationLog log = AlterRoutineLoadJobOperationLog.read(in);
            Assert.assertEquals(7001L, log.getJobId());
            Assert.assertTrue(log.getJobProperties().isEmpty());
            Assert.assertNull(log.getDataSourceProperties());
            Assert.assertNull(log.getRoutineLoadDesc());
        }
    }

    private static byte[] loadBase64Fixture(String resource) throws IOException {
        try (InputStream in = AlterRoutineLoadOperationLogTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing fixture " + resource);
            }
            String base64 = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
            return Base64.getDecoder().decode(base64);
        }
    }

    private static SlotRef namedSlot(String column) {
        SlotRef slotRef = new SlotRef(null, column);
        slotRef.setLabel("`" + column.replace("`", "``") + "`");
        slotRef.setType(Type.VARCHAR);
        return slotRef;
    }

    private static String exprToSql(Expr expr) {
        return expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
    }

}
