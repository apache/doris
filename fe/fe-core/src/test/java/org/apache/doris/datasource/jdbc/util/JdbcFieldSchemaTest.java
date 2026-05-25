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

package org.apache.doris.datasource.jdbc.util;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSetMetaData;
import java.sql.Types;

public class JdbcFieldSchemaTest {

    @Mocked
    private ResultSetMetaData metaData;

    @Test
    public void testUseColumnLabelForQueryAlias() throws Exception {
        new Expectations() {{
                metaData.getColumnLabel(1);
                result = "t1";
                metaData.getColumnType(1);
                result = Types.VARCHAR;
                metaData.getColumnTypeName(1);
                result = "VARCHAR";
                metaData.getPrecision(1);
                result = 64;
                metaData.getScale(1);
                result = 0;
            }};

        JdbcFieldSchema schema = new JdbcFieldSchema(metaData, 1);

        Assert.assertEquals("t1", schema.getColumnName());
    }

    @Test
    public void testFallbackToColumnNameWhenLabelMissing() throws Exception {
        new Expectations() {{
                metaData.getColumnLabel(1);
                result = "";
                metaData.getColumnName(1);
                result = "username";
                metaData.getColumnType(1);
                result = Types.VARCHAR;
                metaData.getColumnTypeName(1);
                result = "VARCHAR";
                metaData.getPrecision(1);
                result = 64;
                metaData.getScale(1);
                result = 0;
            }};

        JdbcFieldSchema schema = new JdbcFieldSchema(metaData, 1);

        Assert.assertEquals("username", schema.getColumnName());
    }
}
