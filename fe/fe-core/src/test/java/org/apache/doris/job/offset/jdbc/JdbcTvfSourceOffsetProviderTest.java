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

package org.apache.doris.job.offset.jdbc;

import org.apache.doris.job.cdc.DataSourceConfigKeys;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JdbcTvfSourceOffsetProviderTest {

    @Test
    public void testEnsureInitializedNormalizesMysqlJdbcUrl() throws Exception {
        JdbcTvfSourceOffsetProvider provider = new JdbcTvfSourceOffsetProvider();
        Map<String, String> properties = new HashMap<>();
        properties.put(DataSourceConfigKeys.TYPE, "mysql");
        properties.put(DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://127.0.0.1:3306/test");
        properties.put(DataSourceConfigKeys.TABLE, "source_table");

        provider.ensureInitialized(1L, properties);

        Assert.assertEquals("jdbc:mysql://127.0.0.1:3306/test?yearIsDateType=false"
                        + "&tinyInt1isBit=false&useUnicode=true&characterEncoding=utf-8",
                provider.getSourceProperties().get(DataSourceConfigKeys.JDBC_URL));
        Assert.assertEquals("jdbc:mysql://127.0.0.1:3306/test",
                properties.get(DataSourceConfigKeys.JDBC_URL));
    }
}
