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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class CreateTableWithBloomFilterIndexTest extends TestWithFeService {
    private static String runningDir = "fe/mocked/CreateTableWithBloomFilterIndexTest/"
            + UUID.randomUUID().toString() + "/";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    private void expectFailureContains(String expectedMessage, ExceptionChecker.ThrowingRunnable runnable) {
        Throwable throwable = Assertions.assertThrows(Throwable.class, runnable::run);
        Assertions.assertTrue(throwable.getMessage().contains(expectedMessage),
                "expected msg: " + expectedMessage + ", actual: " + throwable.getMessage());
    }

    private OlapTable getOlapTable(String tableName) throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        return (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
    }

    private void waitForSchemaChangeDone(String tableName) throws Exception {
        long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (System.currentTimeMillis() < deadlineMs) {
            OlapTable table = getOlapTable(tableName);
            boolean allJobsFinished = true;
            for (AlterJobV2 job : Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values()) {
                if (job.getTableId() == table.getId() && !job.getJobState().isFinalState()) {
                    allJobsFinished = false;
                    break;
                }
            }
            if (table.getState() == OlapTable.OlapTableState.NORMAL && allJobsFinished) {
                return;
            }
            Thread.sleep(100);
        }
        Assertions.fail("schema change job did not finish for table " + tableName);
    }

    private void alterTableSyncAndWait(String sql, String tableName) throws Exception {
        alterTableSync(sql);
        waitForSchemaChangeDone(tableName);
    }

    @Test
    public void testCreateTableWithTinyIntBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "TINYINT is not supported in bloom filter index. invalid column: k1",
                () -> createTable("CREATE TABLE test.tbl_tinyint_bf (\n"
                        + "k1 TINYINT, \n"
                        + "v1 INT\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithSupportedIntBloomFilterIndex() throws Exception {
        // smallint
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_smallint_bf (\n"
                + "k1 SMALLINT, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));

        // int
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_int_bf (\n"
                + "k1 INT, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));

        // bigint
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_bigint_bf (\n"
                + "k1 BIGINT, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));

        // largeint
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_largeint_bf (\n"
                + "k1 LARGEINT, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithFloatBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "FLOAT is not supported in bloom filter index. invalid column: k2",
                () -> createTable("CREATE TABLE test.tbl_float_bf (\n"
                        + "k1 INT, \n"
                        + "k2 FLOAT, \n"
                        + "v1 INT\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k2\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithDoubleBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "DOUBLE is not supported in bloom filter index. invalid column: k2",
                () -> createTable("CREATE TABLE test.tbl_double_bf (\n"
                        + "k1 INT, \n"
                        + "k2 DOUBLE, \n"
                        + "v1 INT\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k2\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithDecimalBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_decimal_bf (\n"
                + "k1 DECIMAL(10,2), \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithCharBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_char_bf (\n"
                + "k1 CHAR(20), \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithVarcharBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_varchar_bf (\n"
                + "k1 VARCHAR(20), \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithTextBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_text_bf (\n"
                + "k1 INT, \n"
                + "k2 TEXT, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k2\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithDecimalV3BloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_decimalv3_bf (\n"
                + "k1 DECIMALV3(10,2), \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithIPv4BloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_ipv4_bf (\n"
                + "k1 IPV4, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithIPv6BloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_ipv6_bf (\n"
                + "k1 IPV6, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithDateBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_date_bf (\n"
                + "k1 DATE, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithDateTimeBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_datetime_bf (\n"
                + "k1 DATETIME, \n"
                + "v1 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithCharNgramBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_char_ngram_bf (\n"
                + "k1 CHAR(20), \n"
                + "v1 INT,\n"
                + "INDEX idx_k1_ngram (k1) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"3\", \"bf_size\"=\"1024\")\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithVarcharNgramBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_varchar_ngram_bf (\n"
                + "k1 VARCHAR(50), \n"
                + "v1 INT,\n"
                + "INDEX idx_k1_ngram (k1) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"3\", \"bf_size\"=\"1024\")\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithStringNgramBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_string_ngram_bf (\n"
                + "k1 INT, \n"
                + "k2 STRING, \n"
                + "v1 INT,\n"
                + "INDEX idx_k2_ngram (k2) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"3\", \"bf_size\"=\"1024\")\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithArrayNumericBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                        "ARRAY is not supported in bloom filter index. invalid column: k1",
                        () -> createTable("CREATE TABLE test.tbl_array_numeric_bf (\n"
                + "v1 INT,\n"
                + "k1 ARRAY<INT>\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(v1)\n"
                + "DISTRIBUTED BY HASH(v1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithArrayDateBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                        "ARRAY is not supported in bloom filter index. invalid column: k1",
                        () -> createTable("CREATE TABLE test.tbl_array_date_bf (\n"
                + "v1 INT,\n"
                + "k1 ARRAY<DATE>\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(v1)\n"
                + "DISTRIBUTED BY HASH(v1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateTableWithArrayStringNgramBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "is not supported in ngram_bf index. invalid column: k1",
                () -> createTable("CREATE TABLE test.tbl_array_string_ngram_bf (\n"
                        + "v1 INT,\n"
                        + "k1 ARRAY<STRING>,\n"
                        + "INDEX idx_k1_ngram (k1) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"3\", \"bf_size\"=\"1024\")\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(v1)\n"
                        + "DISTRIBUTED BY HASH(v1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithMapBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "MAP is not supported in bloom filter index. invalid column: k1",
                () -> createTable("CREATE TABLE test.tbl_map_bf (\n"
                        + "v1 INT,\n"
                        + "k1 MAP<INT, STRING>\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(v1)\n"
                        + "DISTRIBUTED BY HASH(v1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithStructBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "STRUCT is not supported in bloom filter index. invalid column: k1",
                () -> createTable("CREATE TABLE test.tbl_struct_bf (\n"
                        + "v1 INT,\n"
                        + "k1 STRUCT<f1:INT, f2:STRING>\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(v1)\n"
                        + "DISTRIBUTED BY HASH(v1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithJsonBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                " JSON is not supported in bloom filter index. invalid column: k1",
                () -> createTable("CREATE TABLE test.tbl_json_bf (\n"
                        + "v1 INT,\n"
                        + "k1 JSON\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(v1)\n"
                        + "DISTRIBUTED BY HASH(v1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithHllBloomFilterIndex() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                " HLL is not supported in bloom filter index. invalid column: k1",
                () -> createTable("CREATE TABLE test.tbl_hll_bf (\n"
                        + "v1 INT,\n"
                        + "k1 HLL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(v1)\n"
                        + "DISTRIBUTED BY HASH(v1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateMowTableWithBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_mow_bf (\n"
                + "k1 INT, \n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"bloom_filter_columns\" = \"v1\",\n"
                + "\"enable_unique_key_merge_on_write\" = \"true\"\n"
                + ");"));
    }

    @Test
    public void testCreateDuplicateTableWithBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_duplicate_bf (\n"
                + "k1 INT, \n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"v1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testCreateMorTableWithBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_mor_bf (\n"
                + "k1 INT, \n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"bloom_filter_columns\" = \"v1\",\n"
                + "\"enable_unique_key_merge_on_write\" = \"false\"\n"
                + ");"));
    }

    @Test
    public void testCreateAggTableWithBloomFilterIndex() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_agg_bf (\n"
                + "k1 INT, \n"
                + "v1 INT SUM\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testBloomFilterColumnsValidCharacters() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_bf_valid_chars (\n"
                + "k1 INT, \n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1,v1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testBloomFilterColumnsInvalidCharacters() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter column does not exist in table. invalid column: k1;v1",
                () -> createTable("CREATE TABLE test.tbl_bf_invalid_chars (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1;v1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterFppValidInput() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_bf_fpp_valid (\n"
                + "k1 INT, \n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"v1\",\n"
                + "\"bloom_filter_fpp\" = \"0.05\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testBloomFilterFppInvalidInput() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter fpp should in [1.0E-4, 0.05]",
                () -> createTable("CREATE TABLE test.tbl_bf_fpp_invalid (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"v1\",\n"
                        + "\"bloom_filter_fpp\" = \"-0.05\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testNgramBloomFilterGramSizeValidInput() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_ngram_gramsize_valid (\n"
                + "k1 INT, \n"
                + "k2 STRING, \n"
                + "v1 INT,\n"
                + "INDEX idx_k2_ngram (k2) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"4\")\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testNgramBloomFilterGramSizeInvalidInput() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "'gram_size' should be an integer between 1 and 255",
                () -> createTable("CREATE TABLE test.tbl_ngram_gramsize_invalid (\n"
                        + "k1 INT, \n"
                        + "k2 STRING, \n"
                        + "v1 INT,\n"
                        + "INDEX idx_k2_ngram (k2) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"-1\")\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testNgramBloomFilterGramSizeInvalidInput256() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "'gram_size' should be an integer between 1 and 255",
                () -> createTable("CREATE TABLE test.tbl_ngram_gram_size_invalid (\n"
                        + "k1 INT, \n"
                        + "k2 STRING, \n"
                        + "v1 INT,\n"
                        + "INDEX idx_k2_ngram (k2) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"256\")\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testNgramBloomFilterBfSizeValidInput() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "'bf_size' should be an integer between 64 and 65535",
                () -> createTable("CREATE TABLE test.tbl_ngram_bfsize_valid (\n"
                        + "k1 INT, \n"
                        + "k2 STRING, \n"
                        + "v1 INT,\n"
                        + "INDEX idx_k2_ngram (k2) USING NGRAM_BF PROPERTIES(\"bf_size\"=\"256000000\")\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testNgramBloomFilterBfSizeInvalidInput() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "'bf_size' should be an integer between 64 and 65535",
                () -> createTable("CREATE TABLE test.tbl_ngram_bfsize_invalid (\n"
                        + "k1 INT, \n"
                        + "k2 STRING, \n"
                        + "v1 INT,\n"
                        + "INDEX idx_k2_ngram (k2) USING NGRAM_BF PROPERTIES(\"bf_size\"=\"-256000000\")\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testNgramBloomFilterBfSizeInvalidInput65536() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "'bf_size' should be an integer between 64 and 65535",
                () -> createTable("CREATE TABLE test.tbl_ngram_bf_size_invalid (\n"
                        + "k1 INT, \n"
                        + "k2 STRING, \n"
                        + "v1 INT,\n"
                        + "INDEX idx_k2_ngram (k2) USING NGRAM_BF PROPERTIES(\"bf_size\"=\"65536\")\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                        + "PROPERTIES (\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnsDuplicated() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Reduplicated bloom filter column: k1",
                () -> createTable("CREATE TABLE test.tbl_bf_duplicated_columns (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1,k1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnDoesNotExist() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter column does not exist in table. invalid column: k3",
                () -> createTable("CREATE TABLE test.tbl_bf_column_not_exist (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k3\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnInvalidType() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "BOOLEAN is not supported in bloom filter index. invalid column: k2",
                () -> createTable("CREATE TABLE test.tbl_bf_invalid_type (\n"
                        + "k1 INT, \n"
                        + "k2 BOOLEAN,\n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k2\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnNonKeyInAggKeys() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                        "Bloom filter index should only be used in columns of UNIQUE_KEYS/DUP_KEYS table or key columns of AGG_KEYS table. invalid column: v1",
                        () -> createTable("CREATE TABLE test.tbl_bf_nonkey_in_agg (\n"
                + "k1 INT, \n"
                + "v1 INT SUM\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"v1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testBloomFilterFppNotDouble() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter fpp is not Double",
                () -> createTable("CREATE TABLE test.tbl_bf_fpp_not_double (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"v1\",\n"
                        + "\"bloom_filter_fpp\" = \"abc\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterFppOutOfRange() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter fpp should in [1.0E-4, 0.05]",
                () -> createTable("CREATE TABLE test.tbl_bf_fpp_out_of_range (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"v1\",\n"
                        + "\"bloom_filter_fpp\" = \"0.1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterFppBelowMin() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter fpp should in [1.0E-4, 0.05]",
                () -> createTable("CREATE TABLE test.tbl_bf_fpp_below_min (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"v1\",\n"
                        + "\"bloom_filter_fpp\" = \"1e-5\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnsEmptyString() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                        "Unknown properties: {bloom_filter_columns=}",
                        () -> createTable("CREATE TABLE test.tbl_bf_empty_columns (\n"
                + "k1 INT, \n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testBloomFilterColumnsOnlyCommas() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown properties: {bloom_filter_columns=,,,}",
                () -> createTable("CREATE TABLE test.tbl_bf_only_commas (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \",,,\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnsNonExistingColumns() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter column does not exist in table. invalid column: k3",
                () -> createTable("CREATE TABLE test.tbl_bf_non_existing_columns (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20),\n"
                        + "k2 INT\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k2,k3\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnsWithSpecialCharacters() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter column does not exist in table. invalid column: k1@",
                () -> createTable("CREATE TABLE test.tbl_bf_special_chars (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20),\n"
                        + "k2 INT\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1@,v1#\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnsWithDifferentCase() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_bf_different_case (\n"
                + "k1 INT, \n"
                + "V1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(K1)\n"
                + "DISTRIBUTED BY HASH(K1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1,v1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testBloomFilterColumnsWithSpaces() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> createTable("CREATE TABLE test.tbl_bf_columns_with_spaces (\n"
                + "k1 INT, \n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"  k1  ,  v1  \",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");"));
    }

    @Test
    public void testBloomFilterColumnsWithLongColumnName() throws Exception {
        StringBuilder sb = new StringBuilder("k");
        for (int i = 0; i < 1000; i++) {
            sb.append('1');
        }
        String longColumnName = sb.toString();

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter column does not exist in table. invalid column: " + longColumnName,
                () -> createTable("CREATE TABLE test.tbl_bf_long_column_name (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20),\n"
                        + "k2 INT\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"" + longColumnName + "\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnsWithUnicodeCharacters() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter column does not exist in table. invalid column: 名字",
                () -> createTable("CREATE TABLE test.tbl_bf_unicode_columns (\n"
                        + "k1 INT, \n"
                        + "name VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"名字\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testBloomFilterColumnsWithNullOrWhitespace() {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Bloom filter column does not exist in table. invalid column: ",
                () -> createTable("CREATE TABLE test.tbl_bf_null_or_whitespace (\n"
                        + "k1 INT, \n"
                        + "v1 VARCHAR(20)\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \" , \",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableRejectsBfColumnsAndBfIndexOnSameColumn() {
        expectFailureContains("k1 should have only one ngram bloom filter index or bloom filter index",
                () -> createTable("CREATE TABLE test.tbl_bf_conflict_create (\n"
                        + "k1 INT,\n"
                        + "v1 STRING,\n"
                        + "INDEX idx_k1 (k1) USING BLOOMFILTER\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "\"bloom_filter_columns\" = \"k1\",\n"
                        + "\"replication_num\" = \"1\"\n"
                        + ");"));
    }

    @Test
    public void testCreateTableWithBfIndexUsesDefaultFpp() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_named_default_fpp (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        OlapTable table = getOlapTable("tbl_bf_named_default_fpp");
        Assertions.assertNull(table.getCopiedBfColumns());
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("v1"));
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertTrue(table.getIndexes().stream()
                .filter(index -> "idx_v1".equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("bfIndex not found"))
                .getProperties()
                .isEmpty());
    }

    @Test
    public void testCreateTableWithBfIndexUsesIndexLevelFppProperty() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_named_index_level_fpp (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER PROPERTIES (\"bloom_filter_fpp\" = \"0.02\")\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        OlapTable table = getOlapTable("tbl_bf_named_index_level_fpp");
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertEquals("0.02", table.getIndexes().stream()
                .filter(index -> "idx_v1".equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("bfIndex not found"))
                .getProperties()
                .get("bloom_filter_fpp"));
    }

    @Test
    public void testCreateTableWithBfIndexDoesNotUseTableLevelFpp() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_named_custom_fpp (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_fpp\" = \"0.01\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        OlapTable table = getOlapTable("tbl_bf_named_custom_fpp");
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("v1"));
        Assertions.assertTrue(table.getIndexes().stream()
                .filter(index -> "idx_v1".equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("bfIndex not found"))
                .getProperties()
                .isEmpty());
    }

    @Test
    public void testCreateIndexRejectsBfColumnsColumn() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_conflict_add_index (\n"
                + "k1 INT,\n"
                + "v1 STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        boolean enableAddIndexForNewData = connectContext.getSessionVariable().isEnableAddIndexForNewData();
        connectContext.getSessionVariable().setEnableAddIndexForNewData(true);
        try {
            expectFailureContains("k1 should have only one ngram bloom filter index or bloom filter index",
                    () -> alterTableSync("ALTER TABLE test.tbl_bf_conflict_add_index "
                            + "ADD INDEX idx_k1(k1) USING BLOOMFILTER"));
        } finally {
            connectContext.getSessionVariable().setEnableAddIndexForNewData(enableAddIndexForNewData);
        }
    }

    @Test
    public void testLightIndexChangeValidatesFinalIndexSet() throws Exception {
        String tableName = "tbl_bf_replace_ngram_light";
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1_ngram(v1) USING NGRAM_BF "
                + "PROPERTIES(\"gram_size\" = \"2\", \"bf_size\" = \"256\")\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");

        boolean enableAddIndexForNewData = connectContext.getSessionVariable().isEnableAddIndexForNewData();
        connectContext.getSessionVariable().setEnableAddIndexForNewData(true);
        try {
            alterTableSync("ALTER TABLE test." + tableName
                    + " ADD INDEX idx_v1_bf(v1) USING BLOOMFILTER, DROP INDEX idx_v1_ngram");
        } finally {
            connectContext.getSessionVariable().setEnableAddIndexForNewData(enableAddIndexForNewData);
        }

        OlapTable table = getOlapTable(tableName);
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
        Assertions.assertEquals(1, table.getIndexes().size());
        Assertions.assertEquals("idx_v1_bf", table.getIndexes().get(0).getIndexName());
        Assertions.assertEquals(IndexType.BLOOMFILTER, table.getIndexes().get(0).getIndexType());
    }

    @Test
    public void testCreateFirstBfIndexViaAlterUsesDefaultFpp() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_add_first_named (\n"
                + "k1 INT,\n"
                + "v1 STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        alterTableSyncAndWait("ALTER TABLE test.tbl_bf_add_first_named "
                + "ADD INDEX idx_v1(v1) USING BLOOMFILTER", "tbl_bf_add_first_named");

        OlapTable table = getOlapTable("tbl_bf_add_first_named");
        Assertions.assertNull(table.getCopiedBfColumns());
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("v1"));
        Assertions.assertTrue(table.getIndexes().stream()
                .filter(index -> "idx_v1".equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("bfIndex not found"))
                .getProperties()
                .isEmpty());
    }

    @Test
    public void testAddBfIndexUsesLightSchemaChangeWhenEnabled() throws Exception {
        String tableName = "tbl_bf_index_light";
        createTable("CREATE TABLE test." + tableName + " (\n"
                + "k1 INT,\n"
                + "v1 STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        OlapTable table = getOlapTable(tableName);
        int alterJobCount = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().size();
        int indexChangeJobCount = Env.getCurrentEnv().getSchemaChangeHandler().getIndexChangeJobs().size();
        List<Long> jobIdsBefore = Lists.newArrayList(
                Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().keySet());
        boolean enableAddIndexForNewData = connectContext.getSessionVariable().isEnableAddIndexForNewData();
        connectContext.getSessionVariable().setEnableAddIndexForNewData(true);
        try {
            alterTableSync("ALTER TABLE test." + tableName + " ADD INDEX idx_v1(v1) USING BLOOMFILTER");

            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            Assertions.assertEquals(alterJobCount + 1,
                    Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().size());
            Assertions.assertEquals(indexChangeJobCount,
                    Env.getCurrentEnv().getSchemaChangeHandler().getIndexChangeJobs().size());
            Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("v1"));

            AlterJobV2 addJob = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().values().stream()
                    .filter(job -> job.getTableId() == table.getId())
                    .max((left, right) -> Long.compare(left.getJobId(), right.getJobId()))
                    .orElseThrow(() -> new AssertionError("light add job not found"));
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, addJob.getJobState());

            alterTableSync("ALTER TABLE test." + tableName + " DROP INDEX idx_v1");

            Assertions.assertEquals(OlapTable.OlapTableState.SCHEMA_CHANGE, table.getState());
            Assertions.assertEquals(alterJobCount + 2,
                    Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().size());
            Assertions.assertEquals(indexChangeJobCount,
                    Env.getCurrentEnv().getSchemaChangeHandler().getIndexChangeJobs().size());

            waitForSchemaChangeDone(tableName);
            Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());
            Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).isEmpty());
        } finally {
            connectContext.getSessionVariable().setEnableAddIndexForNewData(enableAddIndexForNewData);
            Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2().keySet().retainAll(jobIdsBefore);
        }
    }

    @Test
    public void testCreateFirstBfIndexViaAlterKeepsIndexLevelFppProperty() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_add_first_named_index_level_fpp (\n"
                + "k1 INT,\n"
                + "v1 STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        alterTableSyncAndWait("ALTER TABLE test.tbl_bf_add_first_named_index_level_fpp "
                + "ADD INDEX idx_v1(v1) USING BLOOMFILTER PROPERTIES (\"bloom_filter_fpp\" = \"0.02\")",
                "tbl_bf_add_first_named_index_level_fpp");

        OlapTable table = getOlapTable("tbl_bf_add_first_named_index_level_fpp");
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertEquals("0.02", table.getIndexes().stream()
                .filter(index -> "idx_v1".equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("bfIndex not found"))
                .getProperties()
                .get("bloom_filter_fpp"));
    }

    @Test
    public void testAddSecondBfIndexSucceeds() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_add_second_named (\n"
                + "k1 INT,\n"
                + "k2 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        alterTableSyncAndWait("ALTER TABLE test.tbl_bf_add_second_named "
                + "ADD INDEX idx_k2(k2) USING BLOOMFILTER", "tbl_bf_add_second_named");

        OlapTable table = getOlapTable("tbl_bf_add_second_named");
        Assertions.assertNull(table.getCopiedBfColumns());
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("v1"));
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("k2"));
        Assertions.assertEquals(2, Index.getBfIndexColumns(table.getIndexes()).size());
    }

    @Test
    public void testAlterTableSetBloomFilterColumnsRejectsBfIndexColumn() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_named_alter_conflict (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        expectFailureContains(
                "v1 should have only one ngram bloom filter index or bloom filter index",
                () -> alterTableSync("ALTER TABLE test.tbl_bf_named_alter_conflict "
                        + "SET (\"bloom_filter_columns\" = \"k1,v1\")"));
    }

    @Test
    public void testAlterTableSetBloomFilterColumnsRejectsBfIndexColumn2() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_named_alter_conflict2 (\n"
                + "k1 INT,\n"
                + "k2 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER,\n"
                + "INDEX idx_k2 (k2) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        expectFailureContains(
                "k2 should have only one ngram bloom filter index or bloom filter index",
                () -> alterTableSync("ALTER TABLE test.tbl_bf_named_alter_conflict2 "
                        + "SET (\"bloom_filter_columns\" = \"k2\")"));
    }

    @Test
    public void testAlterTableSetBloomFilterFppOnBfIndexOnlyTableThrowsNoChange() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_named_alter_fpp (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        OlapTable table = getOlapTable("tbl_bf_named_alter_fpp");
        Assertions.assertNull(table.getCopiedBfColumns());
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("v1"));
        expectFailureContains("Bloom filter index has no change",
                () -> alterTableSync("ALTER TABLE test.tbl_bf_named_alter_fpp "
                        + "SET (\"bloom_filter_fpp\" = \"0.01\")"));
        Assertions.assertTrue(getOlapTable("tbl_bf_named_alter_fpp").getIndexes().stream()
                .filter(index -> "idx_v1".equalsIgnoreCase(index.getIndexName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("bfIndex not found"))
                .getProperties()
                .isEmpty());
    }

    @Test
    public void testAlterTableSetSameBloomFilterFppOnBfIndexOnlyTableThrowsNoChange() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_named_same_fpp (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        expectFailureContains("Bloom filter index has no change",
                () -> alterTableSync("ALTER TABLE test.tbl_bf_named_same_fpp "
                        + "SET (\"bloom_filter_fpp\" = \"" + FeConstants.default_bloom_filter_fpp + "\")"));
    }

    @Test
    public void testAlterTableSetBloomFilterFppWithoutAnyBloomFilterThrowsNoChange() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_no_index_alter_fpp (\n"
                + "k1 INT,\n"
                + "v1 STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        expectFailureContains("Bloom filter index has no change",
                () -> alterTableSync("ALTER TABLE test.tbl_bf_no_index_alter_fpp "
                        + "SET (\"bloom_filter_fpp\" = \"0.01\")"));
    }

    @Test
    public void testAlterTableSetSameBfColumnsThrowsNoChange() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_same_legacy_columns (\n"
                + "k1 INT,\n"
                + "v1 STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        expectFailureContains("Bloom filter index has no change",
                () -> alterTableSync("ALTER TABLE test.tbl_bf_same_legacy_columns "
                        + "SET (\"bloom_filter_columns\" = \"k1\")"));
    }

    @Test
    public void testDropIndexRejectsBfColumnsColumn() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_drop_legacy (\n"
                + "k1 INT,\n"
                + "v1 STRING\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        expectFailureContains("index k1 does not exist",
                () -> alterTableSync("ALTER TABLE test.tbl_bf_drop_legacy DROP INDEX k1"));
    }

    @Test
    public void testDropLastBfIndexClearsBfFpp() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_drop_last_named (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        alterTableSyncAndWait("ALTER TABLE test.tbl_bf_drop_last_named DROP INDEX idx_v1",
                "tbl_bf_drop_last_named");

        OlapTable table = getOlapTable("tbl_bf_drop_last_named");
        Assertions.assertNull(table.getCopiedBfColumns());
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).isEmpty());
        Assertions.assertNull(table.getCopiedBfColumns());
        Assertions.assertEquals(0, table.getBfFpp(), 0);
        Assertions.assertTrue(table.getIndexes().isEmpty());
    }

    @Test
    public void testShowCreateTableKeepsBfIndexOutOfBfColumnsProperties() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_show_create (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "INDEX idx_v1 (v1) USING BLOOMFILTER\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("tbl_bf_show_create", Table.TableType.OLAP);
        List<String> createTableStmt = Lists.newArrayList();
        List<String> addRollupStmt = Lists.newArrayList();
        Env.getDdlStmt(table, createTableStmt, null, addRollupStmt, false, true, -1L);

        String ddl = createTableStmt.get(0);
        Assertions.assertTrue(ddl.contains("\"bloom_filter_columns\" = \"k1\""));
        Assertions.assertFalse(ddl.contains("\"bloom_filter_columns\" = \"k1, v1\""));
        Assertions.assertFalse(ddl.contains("\"bloom_filter_columns\" = \"v1\""));
        Assertions.assertTrue(ddl.contains("INDEX idx_v1 (`v1`) USING BLOOMFILTER"));
        Assertions.assertTrue(table.getCopiedBfColumns().contains("k1"));
        Assertions.assertTrue(Index.getBfIndexColumns(table.getIndexes()).contains("v1"));
        Assertions.assertEquals(1, table.getCopiedBfColumns().size());
    }

    @Test
    public void testRenameBfColumnsColumnUpdatesMetadata() throws Exception {
        // Keep one untouched BfColumns entry so renameColumn() has to rewrite the renamed entry
        // and preserve the other one in the same metadata set.
        createTable("CREATE TABLE test.tbl_bf_rename_legacy (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "v2 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1,v1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        alterTableSyncAndWait("ALTER TABLE test.tbl_bf_rename_legacy RENAME COLUMN v1 v1_new",
                "tbl_bf_rename_legacy");

        OlapTable table = getOlapTable("tbl_bf_rename_legacy");
        Assertions.assertTrue(table.getCopiedBfColumns().contains("k1"));
        Assertions.assertFalse(table.getCopiedBfColumns().contains("v1"));
        Assertions.assertTrue(table.getCopiedBfColumns().contains("v1_new"));
    }

    @Test
    public void testDropBfColumnsColumnUpdatesMetadata() throws Exception {
        createTable("CREATE TABLE test.tbl_bf_drop_legacy_column (\n"
                + "k1 INT,\n"
                + "v1 STRING,\n"
                + "v2 INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"bloom_filter_columns\" = \"k1,v1\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ");");

        alterTableSyncAndWait("ALTER TABLE test.tbl_bf_drop_legacy_column DROP COLUMN v1",
                "tbl_bf_drop_legacy_column");

        OlapTable table = getOlapTable("tbl_bf_drop_legacy_column");
        Assertions.assertTrue(table.getCopiedBfColumns().contains("k1"));
        Assertions.assertFalse(table.getCopiedBfColumns().contains("v1"));
        Assertions.assertEquals(1, table.getCopiedBfColumns().size());
    }
}
