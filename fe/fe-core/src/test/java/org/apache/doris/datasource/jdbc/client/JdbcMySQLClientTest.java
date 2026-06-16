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

package org.apache.doris.datasource.jdbc.client;

import org.junit.Assert;
import org.junit.Test;

public class JdbcMySQLClientTest {

    @Test
    public void testIsDorisCompatibleVersionComment() {
        Assert.assertTrue(JdbcMySQLClient.isDorisCompatibleVersionComment("Apache Doris version 3.1.0"));
        Assert.assertTrue(JdbcMySQLClient.isDorisCompatibleVersionComment("SelectDB Cloud version 4.0.5"));
        Assert.assertTrue(JdbcMySQLClient.isDorisCompatibleVersionComment("VeloDB version 2.1.0"));
        Assert.assertTrue(JdbcMySQLClient.isDorisCompatibleVersionComment(
                "enterprise version enterprise-4.0.5-rc01-0724569463d (Cloud Mode)"));

        Assert.assertFalse(JdbcMySQLClient.isDorisCompatibleVersionComment("MySQL Community Server - GPL"));
        Assert.assertFalse(JdbcMySQLClient.isDorisCompatibleVersionComment(""));
        Assert.assertFalse(JdbcMySQLClient.isDorisCompatibleVersionComment(null));
    }
}
