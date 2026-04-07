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

package org.apache.doris.common.util;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FilePartitionUtils;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class BrokerUtilTest {

    @Test
    public void parseColumnsFromPath() {
        String path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assert.assertEquals(1, columns.size());
            Assert.assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k2"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assert.assertEquals(1, columns.size());
            Assert.assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2=v2/a/k1=v1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2", "k3"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k2=v2//k1=v1//xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1//xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.assertEquals(2, columns.size());
            Assert.assertEquals(Lists.newArrayList("v1", "=v2="), columns);
        } catch (UserException e) {
            Assert.fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1/";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assert.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=2/a/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
        } catch (UserException ignored) {
            Assert.fail();
        }

    }

}
