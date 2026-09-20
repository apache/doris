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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TGroupCommitMode;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTableSink;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class GroupCommitBlockSinkTest {

    @Test
    public void testInitLocationParamsSkipsCreateLocation() throws UserException {
        OlapTable dstTable = Mockito.mock(OlapTable.class);
        TupleDescriptor tuple = Mockito.mock(TupleDescriptor.class);
        GroupCommitBlockSink sink = new GroupCommitBlockSink(
                dstTable, tuple, Lists.newArrayList(1L), false, "async_mode", 0.0);

        List<TOlapTableLocationParam> params = sink.initLocationParams(new TOlapTableSink());

        Assert.assertEquals(2, params.size());
        Assert.assertNotNull(params.get(0).getTablets());
        Assert.assertTrue("master location should be empty placeholder",
                params.get(0).getTablets().isEmpty());
        Assert.assertNotNull(params.get(1).getTablets());
        Assert.assertTrue("slave location should be empty placeholder",
                params.get(1).getTablets().isEmpty());
        Mockito.verifyNoInteractions(dstTable);
        Mockito.verifyNoInteractions(tuple);
    }

    @Test
    public void testParseGroupCommit() {
        Assert.assertEquals(TGroupCommitMode.ASYNC_MODE,
                GroupCommitBlockSink.parseGroupCommit("async_mode"));
        Assert.assertEquals(TGroupCommitMode.ASYNC_MODE,
                GroupCommitBlockSink.parseGroupCommit("ASYNC_MODE"));
        Assert.assertEquals(TGroupCommitMode.SYNC_MODE,
                GroupCommitBlockSink.parseGroupCommit("sync_mode"));
        Assert.assertEquals(TGroupCommitMode.OFF_MODE,
                GroupCommitBlockSink.parseGroupCommit("off_mode"));
        Assert.assertNull(GroupCommitBlockSink.parseGroupCommit(null));
        Assert.assertNull(GroupCommitBlockSink.parseGroupCommit("invalid"));
    }
}
