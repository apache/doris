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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

/**
 * Tests for FILE type restrictions in Nereids analyzer.
 * FILE type columns should not be used in ORDER BY, GROUP BY, JOIN equal conditions,
 * or window function PARTITION BY / ORDER BY.
 */
public class CheckFileTypeTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_file_type");
        connectContext.setDatabase("test_file_type");
        createTable("CREATE TABLE fileset_tbl (\n"
                + "    `file` FILE NULL\n"
                + ") ENGINE = fileset\n"
                + "PROPERTIES (\n"
                + "    'location' = 's3://test-bucket/data/*',\n"
                + "    's3.region' = 'us-east-1',\n"
                + "    's3.endpoint' = 'https://s3.us-east-1.amazonaws.com',\n"
                + "    's3.access_key' = 'test_ak',\n"
                + "    's3.secret_key' = 'test_sk'\n"
                + ")");
    }

    @Test
    public void testOrderByFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT `file` FROM fileset_tbl ORDER BY `file`"
                ).rewrite());
    }

    @Test
    public void testOrderByWithLimitFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT `file` FROM fileset_tbl ORDER BY `file` LIMIT 10"
                ).rewrite());
    }

    @Test
    public void testGroupByFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT `file` FROM fileset_tbl GROUP BY `file`"
                ).rewrite());
    }

    @Test
    public void testDistinctFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT DISTINCT `file` FROM fileset_tbl"
                ).rewrite());
    }

    @Test
    public void testMinFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT MIN(`file`) FROM fileset_tbl"
                ));
    }

    @Test
    public void testMaxFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT MAX(`file`) FROM fileset_tbl"
                ));
    }

    @Test
    public void testJoinOnFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                "file type could not in join equal conditions",
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT * FROM fileset_tbl t1 "
                                + "JOIN fileset_tbl t2 ON t1.`file` = t2.`file`"
                ).rewrite());
    }

    @Test
    public void testWindowOrderByFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT row_number() OVER (ORDER BY `file`) FROM fileset_tbl"
                ).rewrite());
    }

    @Test
    public void testWindowPartitionByFileType() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                Type.OnlyMetricTypeErrorMsg,
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT row_number() OVER (PARTITION BY `file`) FROM fileset_tbl"
                ).rewrite());
    }
}
