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

package org.apache.doris.qe;

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ShowResultSetTest {
    @Mocked
    ShowResultSetMetaData metaData;
    @Test
    public void testNormal() {
        List<List<String>> rows = Lists.newArrayList();

        rows.add(Lists.newArrayList("col1-0", "col2-0"));
        rows.add(Lists.newArrayList("123", "456"));
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        Assert.assertEquals(rows, resultSet.getResultRows());
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1-0", resultSet.getString(0));
        Assert.assertEquals("col2-0", resultSet.getString(1));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(123, resultSet.getInt(0));
        Assert.assertEquals(456, resultSet.getLong(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOutOfBound() {
        List<List<String>> rows = Lists.newArrayList();

        rows.add(Lists.newArrayList("col1-0", "col2-0"));
        rows.add(Lists.newArrayList("123", "456"));
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        resultSet.getString(0);
        Assert.fail("No exception throws.");
    }

    @Test(expected = NumberFormatException.class)
    public void testBadNumber() {
        List<List<String>> rows = Lists.newArrayList();

        rows.add(Lists.newArrayList(" 123", "456"));
        ShowResultSet resultSet = new ShowResultSet(metaData, rows);
        resultSet.next();
        resultSet.getInt(0);
        Assert.fail("No exception throws.");
    }
}