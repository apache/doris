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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConvertTz;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.JsonObject;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NullIf;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for Snowflake-compatible functions.
 * <p>
 * Phase 1: Alias functions (IFF, ARRAY_CONSTRUCT, ARRAY_CAT, etc.)
 * Phase 2: RewriteWhenAnalyze functions (ZEROIFNULL, NULLIFZERO, NVL2, EQUAL_NULL,
 * CONVERT_TIMEZONE, OBJECT_CONSTRUCT, TO_VARCHAR)
 */
public class SnowflakeCompatFunctionTest extends TestWithFeService implements PlanPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE test.sf_t (\n"
                + "    id INT NULL,\n"
                + "    val BIGINT NULL,\n"
                + "    name VARCHAR(100) NULL,\n"
                + "    ts DATETIME NULL\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")");
    }

    // ==================== Phase 1: Alias function tests ====================

    @Test
    public void testIff() {
        // IFF is alias for IF
        PlanChecker.from(connectContext).analyze("select iff(true, 1, 2)");
    }

    @Test
    public void testArrayConstruct() {
        // ARRAY_CONSTRUCT is alias for ARRAY
        PlanChecker.from(connectContext).analyze("select array_construct(1, 2, 3)");
    }

    @Test
    public void testArrayCat() {
        // ARRAY_CAT is alias for ARRAY_CONCAT
        PlanChecker.from(connectContext).analyze("select array_cat(array(1,2), array(3,4))");
    }

    @Test
    public void testArrayToString() {
        // ARRAY_TO_STRING is alias for ARRAY_JOIN
        PlanChecker.from(connectContext).analyze("select array_to_string(array('a','b','c'), ',')");
    }

    @Test
    public void testParseJson() {
        // PARSE_JSON is alias for JSONB_PARSE
        PlanChecker.from(connectContext).analyze("select parse_json('{\"a\":1}')");
    }

    @Test
    public void testTryParseJson() {
        // TRY_PARSE_JSON is alias for JSONB_PARSE_ERROR_TO_NULL
        PlanChecker.from(connectContext).analyze("select try_parse_json('{bad json}')");
    }

    @Test
    public void testCheckJson() {
        // CHECK_JSON is alias for JSON_VALID
        PlanChecker.from(connectContext).analyze("select check_json('{\"a\":1}')");
    }

    @Test
    public void testBase64Encode() {
        // BASE64_ENCODE is alias for TO_BASE64
        PlanChecker.from(connectContext).analyze("select base64_encode('hello')");
    }

    @Test
    public void testBase64DecodeString() {
        // BASE64_DECODE_STRING is alias for FROM_BASE64
        PlanChecker.from(connectContext).analyze("select base64_decode_string('aGVsbG8=')");
    }

    @Test
    public void testHexEncode() {
        // HEX_ENCODE is alias for HEX
        PlanChecker.from(connectContext).analyze("select hex_encode('hello')");
    }

    @Test
    public void testHexDecodeString() {
        // HEX_DECODE_STRING is alias for UNHEX
        PlanChecker.from(connectContext).analyze("select hex_decode_string('68656C6C6F')");
    }

    @Test
    public void testEndswith() {
        // ENDSWITH is alias for ENDS_WITH
        PlanChecker.from(connectContext).analyze("select endswith('hello', 'lo')");
    }

    @Test
    public void testStartswith() {
        // STARTSWITH is alias for STARTS_WITH
        PlanChecker.from(connectContext).analyze("select startswith('hello', 'he')");
    }

    @Test
    public void testGetdate() {
        // GETDATE is alias for NOW
        PlanChecker.from(connectContext).analyze("select getdate()");
    }

    @Test
    public void testLen() {
        // LEN is alias for CHAR_LENGTH (character count, not byte count)
        PlanChecker.from(connectContext).analyze("select len('hello')");
    }

    @Test
    public void testCharindex() {
        // CHARINDEX is alias for LOCATE
        PlanChecker.from(connectContext).analyze("select charindex('ll', 'hello')");
    }

    // ==================== Phase 2: RewriteWhenAnalyze function tests ====================

    @Test
    public void testZeroifnullConstant() {
        // ZEROIFNULL(5) should analyze without error and rewrite to Coalesce
        PlanChecker.from(connectContext)
                .analyze("select zeroifnull(5)")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof Coalesce;
                        })
                );
    }

    @Test
    public void testZeroifnullWithNonNumericShouldFail() {
        Assertions.assertThrows(Exception.class, () ->
                PlanChecker.from(connectContext).analyze("select zeroifnull('hello')")
        );
    }

    @Test
    public void testZeroifnullWithColumn() {
        // ZEROIFNULL on a column should rewrite to Coalesce
        PlanChecker.from(connectContext)
                .analyze("select zeroifnull(val) from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof Coalesce;
                        })
                );
    }

    @Test
    public void testNullifzeroConstant() {
        // NULLIFZERO(0) should analyze without error and rewrite to NullIf
        PlanChecker.from(connectContext)
                .analyze("select nullifzero(0)")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof NullIf;
                        })
                );
    }

    @Test
    public void testNullifzeroWithNonNumericShouldFail() {
        Assertions.assertThrows(Exception.class, () ->
                PlanChecker.from(connectContext).analyze("select nullifzero('hello')")
        );
    }

    @Test
    public void testNullifzeroWithColumn() {
        PlanChecker.from(connectContext)
                .analyze("select nullifzero(val) from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof NullIf;
                        })
                );
    }

    @Test
    public void testNvl2Constant() {
        // NVL2(expr1, expr2, expr3) should rewrite to IF(NOT(IS_NULL(expr1)), expr2, expr3)
        PlanChecker.from(connectContext)
                .analyze("select nvl2(1, 'yes', 'no')")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof If;
                        })
                );
    }

    @Test
    public void testNvl2WithColumn() {
        PlanChecker.from(connectContext)
                .analyze("select nvl2(name, 'has_name', 'no_name') from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof If;
                        })
                );
    }

    @Test
    public void testEqualNullConstant() {
        // EQUAL_NULL(a, b) should rewrite to NullSafeEqual (a <=> b)
        PlanChecker.from(connectContext)
                .analyze("select equal_null(1, 1)")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof NullSafeEqual;
                        })
                );
    }

    @Test
    public void testEqualNullWithColumn() {
        PlanChecker.from(connectContext)
                .analyze("select equal_null(id, val) from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof NullSafeEqual;
                        })
                );
    }

    @Test
    public void testConvertTimezoneConstant() {
        // CONVERT_TIMEZONE(src_tz, dst_tz, timestamp) should rewrite to ConvertTz(ts, src_tz, dst_tz)
        PlanChecker.from(connectContext)
                .analyze("select convert_timezone('UTC', 'Asia/Shanghai', '2024-01-01 00:00:00')")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof ConvertTz;
                        })
                );
    }

    @Test
    public void testConvertTimezoneWithColumn() {
        PlanChecker.from(connectContext)
                .analyze("select convert_timezone('UTC', 'Asia/Shanghai', ts) from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof ConvertTz;
                        })
                );
    }

    @Test
    public void testObjectConstructConstant() {
        // OBJECT_CONSTRUCT(k1, v1, k2, v2) should rewrite to JsonObject
        PlanChecker.from(connectContext)
                .analyze("select object_construct('name', 'Alice', 'age', '30')")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof JsonObject;
                        })
                );
    }

    @Test
    public void testObjectConstructWithColumn() {
        PlanChecker.from(connectContext)
                .analyze("select object_construct('id', id, 'name', name) from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof JsonObject;
                        })
                );
    }

    @Test
    public void testObjectConstructOddArgsShouldFail() {
        // Odd number of args should throw AnalysisException
        Assertions.assertThrows(Exception.class, () ->
                PlanChecker.from(connectContext).analyze("select object_construct('key1')")
        );
    }

    @Test
    public void testToVarcharOneArg() {
        // TO_VARCHAR(expr) should rewrite to CAST(expr AS VARCHAR)
        PlanChecker.from(connectContext)
                .analyze("select to_varchar(123)")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof Cast;
                        })
                );
    }

    @Test
    public void testToVarcharTwoArgs() {
        // TO_VARCHAR(date_expr, fmt) should rewrite to DateFormat
        PlanChecker.from(connectContext)
                .analyze("select to_varchar(ts, '%Y-%m-%d') from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof DateFormat;
                        })
                );
    }

    @Test
    public void testToVarcharOneArgWithColumn() {
        // TO_VARCHAR on a column → CAST
        PlanChecker.from(connectContext)
                .analyze("select to_varchar(val) from sf_t")
                .matches(
                        logicalProject().when(proj -> {
                            Expression expr = ((LogicalProject<?>) proj).getProjects().get(0).child(0);
                            return expr instanceof Cast;
                        })
                );
    }

    @Test
    public void testToChar() {
        // TO_CHAR is a Snowflake alias for TO_VARCHAR; 1-arg form rewrites to Cast
        PlanChecker.from(connectContext)
                .analyze("select to_char(123)")
                .matches(
                        logicalOneRowRelation().when(oneRow -> {
                            Expression expr = oneRow.getProjects().get(0).child(0);
                            return expr instanceof Cast;
                        })
                );
    }
}
