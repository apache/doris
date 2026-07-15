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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.common.DataSourceType;

import org.junit.Assert;
import org.junit.Test;

public class StreamingJdbcUrlNormalizerTest {

    @Test
    public void testNormalizeMysqlJdbcUrl() {
        String jdbcUrl = StreamingJdbcUrlNormalizer.normalize(
                DataSourceType.MYSQL, "jdbc:mysql://127.0.0.1:3306/test");

        Assert.assertEquals("jdbc:mysql://127.0.0.1:3306/test?yearIsDateType=false"
                        + "&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8",
                jdbcUrl);
        Assert.assertFalse(jdbcUrl.contains("rewriteBatchedStatements"));
    }

    @Test
    public void testNormalizeMysqlJdbcUrlOverridesOppositeValues() {
        String jdbcUrl = StreamingJdbcUrlNormalizer.normalize(DataSourceType.MYSQL,
                "jdbc:mysql://127.0.0.1:3306/test?tinyInt1isBit=true"
                        + "&yearIsDateType=true&useUnicode=false&characterEncoding=GBK");

        Assert.assertTrue(jdbcUrl.contains("tinyInt1isBit=false"));
        Assert.assertTrue(jdbcUrl.contains("yearIsDateType=false"));
        Assert.assertTrue(jdbcUrl.contains("useUnicode=true"));
        Assert.assertTrue(jdbcUrl.contains("characterEncoding=GBK&characterEncoding=utf-8"));
        Assert.assertFalse(jdbcUrl.contains("tinyInt1isBit=true"));
        Assert.assertFalse(jdbcUrl.contains("yearIsDateType=true"));
        Assert.assertFalse(jdbcUrl.contains("useUnicode=false"));
        Assert.assertFalse(jdbcUrl.contains("rewriteBatchedStatements"));
    }

    @Test
    public void testNormalizeMysqlJdbcUrlIsIdempotent() {
        String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test?yearIsDateType=false"
                + "&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8";

        Assert.assertEquals(jdbcUrl,
                StreamingJdbcUrlNormalizer.normalize(DataSourceType.MYSQL, jdbcUrl));
    }

    @Test
    public void testNormalizePostgresJdbcUrlDoesNotChange() {
        String jdbcUrl = "jdbc:postgresql://127.0.0.1:5432/test";

        Assert.assertEquals(jdbcUrl,
                StreamingJdbcUrlNormalizer.normalize(DataSourceType.POSTGRES, jdbcUrl));
    }
}
