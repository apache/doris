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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.DeferredFileFormatProperties;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.load.NereidsDataDescription;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoadCommandTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getState().setNereids(true);
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        FeConstants.runningUnitTest = true;

        createDatabase("nereids_load");
        useDatabase("nereids_load");
        String createTableSql = "CREATE TABLE `customer` (\n"
                + "  `custkey` int(11) NOT NULL,\n"
                + "  `c_name` varchar(25) NOT NULL,\n"
                + "  `c_address` varchar(40) NOT NULL,\n"
                + "  `c_nationkey` int(11) NOT NULL,\n"
                + "  `c_phone` varchar(15) NOT NULL,\n"
                + "  `c_acctbal` DECIMAL(15, 2) NOT NULL,\n"
                + "  `c_mktsegment` varchar(10) NOT NULL,\n"
                + "  `c_comment` varchar(117) NOT NULL\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`custkey`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`custkey`) BUCKETS 24\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"function_column.sequence_col\" = \"c_nationkey\","
                + "\"storage_format\" = \"V2\"\n"
                + ");";
        createTable(createTableSql);

        String createTableBitmapSql = "CREATE TABLE `load_bitmap_table`\n"
                + "        (\n"
                + "            `dt_h`      datetime    NOT NULL,\n"
                + "            `userid_bitmap` bitmap BITMAP_UNION NOT NULL\n"
                + "        ) ENGINE = OLAP AGGREGATE KEY(`dt_h`)\n"
                + "        DISTRIBUTED BY HASH(`dt_h`) BUCKETS 4\n"
                + "        PROPERTIES (\"replication_num\" = \"1\");";
        createTable(createTableBitmapSql);
    }

    @Test
    public void testLoadCommandBitmap() {
        String loadSql1 = "LOAD LABEL load_bitmap_table_test( "
                + "     DATA INFILE(\"s3://bucket/load_bitmap_table\") "
                + "     INTO TABLE load_bitmap_table "
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (raw_dt_h,raw_userid_bitmap) "
                + "     SET (`dt_h` = raw_dt_h, `userid_bitmap` = bitmap_from_array(cast(raw_userid_bitmap as ARRAY<BIGINT(20)>)))"
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";

        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());

        // columns
        LoadCommand command = (LoadCommand) statements.get(0).first;
        List<NereidsDataDescription> dataDescriptions = command.getDataDescriptions();
        Assertions.assertFalse(dataDescriptions.isEmpty());
        NereidsDataDescription dataDescription = dataDescriptions.get(0);
        List<String> colNames = dataDescription.getFileFieldNames();
        Assertions.assertEquals(2, colNames.size());
        Assertions.assertTrue(colNames.contains("raw_dt_h"));
        Assertions.assertTrue(colNames.contains("raw_userid_bitmap"));
        Assertions.assertTrue(dataDescription.getColumnMappingList().size() == 2);
        Assertions.assertTrue(dataDescription.getColumnMappingList().get(1).child(0).getExpressionName().contains("userid_bitmap"));
    }

    @Test
    public void testLoadCommand() throws Exception {
        String loadSql1 = "LOAD LABEL customer_lable_for_test( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE custkey > 100"
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";

        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());

        // columns
        LoadCommand command = (LoadCommand) statements.get(0).first;
        List<NereidsDataDescription> dataDescriptions = command.getDataDescriptions();
        Assertions.assertFalse(dataDescriptions.isEmpty());
        NereidsDataDescription dataDescription = dataDescriptions.get(0);
        List<String> colNames = dataDescription.getFileFieldNames();
        Assertions.assertEquals(8, colNames.size());
        Assertions.assertTrue(colNames.contains("c_custkey"));
        Assertions.assertTrue(colNames.contains("c_name"));
        Assertions.assertTrue(colNames.contains("c_address"));
        Assertions.assertTrue(colNames.contains("c_nationkey"));
        Assertions.assertTrue(colNames.contains("c_phone"));
        Assertions.assertTrue(colNames.contains("c_acctbal"));
        Assertions.assertTrue(colNames.contains("c_mktsegment"));
        Assertions.assertTrue(colNames.contains("c_comment"));

        // pre filter
        Expression preFilter = dataDescription.getPrecdingFilterExpr();
        Assertions.assertNotNull(preFilter);
        Assertions.assertTrue(preFilter instanceof EqualTo);
        Assertions.assertTrue(preFilter.child(0) instanceof UnboundSlot);
        Assertions.assertTrue(preFilter.child(1) instanceof StringLikeLiteral);

        // where
        Expression where = dataDescription.getWhereExpr();
        Assertions.assertNotNull(where);
        Assertions.assertTrue(where instanceof GreaterThan);
        Assertions.assertTrue(where.child(0) instanceof UnboundSlot);
        Assertions.assertTrue(where.child(1) instanceof IntegerLikeLiteral);

        // broker desc
        BrokerDesc brokerDesc = command.getBrokerDesc();
        Assertions.assertNotNull(brokerDesc);
        Assertions.assertEquals("S3", brokerDesc.getName());
        Assertions.assertEquals(TFileType.FILE_S3, brokerDesc.getFileType());
        Map<String, String> properties = brokerDesc.getProperties();
        Assertions.assertNotNull(properties);
        Assertions.assertEquals(TFileType.FILE_S3, brokerDesc.getFileType());
        List<String> brokerProperties = new ArrayList<String>() {
            {
                add("s3.access_key");
                add("s3.secret_key");
                add("s3.endpoint");
                add("s3.region");
                add("s3.secret_key");
            }
        };
        properties.forEach((key, value) -> {
            Assertions.assertTrue(brokerProperties.contains(key));
        });

        //property
        Map<String, String> loadProperties = command.getProperties();
        Assertions.assertNotNull(loadProperties);
        Assertions.assertTrue(loadProperties.get("exec_mem_limit").equalsIgnoreCase("8589934592"));

        // label
        LabelName labelName = command.getLabel();
        Assertions.assertNotNull(labelName);
        Assertions.assertEquals("customer_lable_for_test", labelName.getLabelName());

        // comment
        String comment = command.getComment();
        Assertions.assertNotNull(comment);
        Assertions.assertEquals("test", comment);

        // table name
        String tableName = dataDescription.getTableName();
        Assertions.assertNotNull(tableName);
        Assertions.assertEquals("customer", tableName);

        // column separator and line delimiter
        dataDescription.analyzeWithoutCheckPriv("nereids_load");
        DeferredFileFormatProperties fileFormatProperties =
                (DeferredFileFormatProperties) dataDescription.getFileFormatProperties();
        Assertions.assertNotNull(fileFormatProperties);
        fileFormatProperties.deferInit(TFileFormatType.FORMAT_CSV_PLAIN);
        Assertions.assertTrue(fileFormatProperties.getDelegate() instanceof CsvFileFormatProperties);
        CsvFileFormatProperties csvFileFormatProperties = (CsvFileFormatProperties) fileFormatProperties.getDelegate();
        Assertions.assertEquals("|", csvFileFormatProperties.getColumnSeparator());
        Assertions.assertEquals("\n", csvFileFormatProperties.getLineDelimiter());
    }
}
