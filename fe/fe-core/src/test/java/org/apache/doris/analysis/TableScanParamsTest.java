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

package org.apache.doris.analysis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TableScanParamsTest {
    private static final Map<String, String> EMPTY_MAP = ImmutableMap.of();
    private static final List<String> EMPTY_LIST = ImmutableList.of();

    @Test
    public void testConstructAcceptsValidParamTypes() {
        new TableScanParams(TableScanParams.INCREMENTAL_READ, EMPTY_MAP, EMPTY_LIST);
        new TableScanParams(TableScanParams.BRANCH, EMPTY_MAP, EMPTY_LIST);
        new TableScanParams(TableScanParams.TAG, EMPTY_MAP, EMPTY_LIST);
        new TableScanParams(TableScanParams.SNAPSHOT, EMPTY_MAP, EMPTY_LIST);
        new TableScanParams(TableScanParams.RESET, EMPTY_MAP, EMPTY_LIST);
    }

    @Test
    public void testConstructRejectsInvalidParamType() {
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> new TableScanParams("unknown", EMPTY_MAP, EMPTY_LIST));
        Assert.assertTrue(e.getMessage().contains("Invalid param type"));
    }

    @Test
    public void testParamTypeLowerCased() {
        TableScanParams params = new TableScanParams("BRANCH", EMPTY_MAP, EMPTY_LIST);
        Assert.assertEquals(TableScanParams.BRANCH, params.getParamType());
        Assert.assertTrue(params.isBranch());
    }

    @Test
    public void testNullMapParamsBecomesEmpty() {
        TableScanParams params = new TableScanParams(TableScanParams.TAG, null, EMPTY_LIST);
        Assert.assertTrue(params.getMapParams().isEmpty());
    }

    @Test
    public void testTypePredicates() {
        Assert.assertTrue(new TableScanParams(TableScanParams.INCREMENTAL_READ, EMPTY_MAP, EMPTY_LIST)
                .incrementalRead());
        Assert.assertTrue(new TableScanParams(TableScanParams.SNAPSHOT, EMPTY_MAP, EMPTY_LIST).isSnapshot());
        Assert.assertTrue(new TableScanParams(TableScanParams.RESET, EMPTY_MAP, EMPTY_LIST).isReset());
        Assert.assertTrue(new TableScanParams(TableScanParams.TAG, EMPTY_MAP, EMPTY_LIST).isTag());
    }

    @Test
    public void testValidateOlapTableAcceptsIncr() {
        new TableScanParams(TableScanParams.INCREMENTAL_READ, EMPTY_MAP, EMPTY_LIST).validateOlapTable();
    }

    @Test
    public void testValidateOlapTableRejectsOthers() {
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> new TableScanParams(TableScanParams.BRANCH, EMPTY_MAP, EMPTY_LIST).validateOlapTable());
        Assert.assertTrue(e.getMessage().contains("Invalid param type for olap table"));
    }

    @Test
    public void testValidateOlapTableStreamAcceptsSnapshotAndReset() {
        new TableScanParams(TableScanParams.SNAPSHOT, EMPTY_MAP, EMPTY_LIST).validateOlapTableStream();
        new TableScanParams(TableScanParams.RESET, EMPTY_MAP, EMPTY_LIST).validateOlapTableStream();
    }

    @Test
    public void testValidateOlapTableStreamRejectsOthers() {
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
                () -> new TableScanParams(TableScanParams.INCREMENTAL_READ, EMPTY_MAP, EMPTY_LIST)
                        .validateOlapTableStream());
        Assert.assertTrue(e.getMessage().contains("Invalid param type for olap table stream"));
    }
}
