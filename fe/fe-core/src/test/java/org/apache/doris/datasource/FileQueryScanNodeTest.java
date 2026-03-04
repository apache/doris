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

package org.apache.doris.datasource;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileFormatType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FileQueryScanNodeTest {
    private static final long MB = 1024L * 1024L;

    private static class TestFileQueryScanNode extends FileQueryScanNode {
        TestFileQueryScanNode(SessionVariable sv) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), "test", false, sv);
        }

        @Override
        protected TFileFormatType getFileFormatType() throws UserException {
            return TFileFormatType.FORMAT_ORC;
        }

        @Override
        protected List<String> getPathPartitionKeys() throws UserException {
            return Collections.emptyList();
        }

        @Override
        protected TableIf getTargetTable() throws UserException {
            return null;
        }

        @Override
        protected Map<String, String> getLocationProperties() throws UserException {
            return Collections.emptyMap();
        }
    }

    @Test
    public void testApplyMaxFileSplitNumLimitRaisesTargetSize() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitKeepsTargetSizeWhenSmall() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 500L * MB);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitDisabled() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(0);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(32 * MB, target);
    }
}
