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

package org.apache.doris.mysql;

import org.apache.doris.catalog.MysqlColType;

import org.junit.Assert;
import org.junit.Test;

public class MysqlColTypeTest {

    @Test
    public void testGetCode() {
        MysqlColType type;
        // decimal
        type = MysqlColType.MYSQL_TYPE_DECIMAL;
        Assert.assertEquals(0, type.getCode());
        Assert.assertEquals("DECIMAL", type.toString());

        // tiny
        type = MysqlColType.MYSQL_TYPE_TINY;
        Assert.assertEquals(1, type.getCode());
        Assert.assertEquals("TINY INT", type.toString());

        // SHORT
        type = MysqlColType.MYSQL_TYPE_SHORT;
        Assert.assertEquals(2, type.getCode());
        Assert.assertEquals("SMALL INT", type.toString());

        // LONG
        type = MysqlColType.MYSQL_TYPE_LONG;
        Assert.assertEquals(3, type.getCode());
        Assert.assertEquals("INT", type.toString());

        // FLOAT
        type = MysqlColType.MYSQL_TYPE_FLOAT;
        Assert.assertEquals(4, type.getCode());
        Assert.assertEquals("FLOAT", type.toString());

        // DOUBLE
        type = MysqlColType.MYSQL_TYPE_DOUBLE;
        Assert.assertEquals(5, type.getCode());
        Assert.assertEquals("DOUBLE", type.toString());

        // NULL
        type = MysqlColType.MYSQL_TYPE_NULL;
        Assert.assertEquals(6, type.getCode());
        Assert.assertEquals("NULL", type.toString());
    }

}
