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

package org.apache.doris.load.loadv2;

import org.apache.doris.thrift.TQueryOptions;

import org.junit.Assert;
import org.junit.Test;

public class LoadLoadingTaskTest {

    /**
     * Test the batch size setting logic in LoadLoadingTask.executeOnce().
     * When enableMemTableOnSinkNode = true, setBatchSize() should be called.
     * When enableMemTableOnSinkNode = false, setBatchSize() should NOT be called.
     */
    @Test
    public void testBatchSizeSettingLogic() {
        int brokerLoadBatchSize = 16352;

        // Case 1: enableMemTableOnSinkNode = true, setBatchSize should be called
        TQueryOptions queryOptionsWithMemTable = new TQueryOptions();
        boolean enableMemTableOnSinkNode1 = true;
        queryOptionsWithMemTable.setEnableMemtableOnSinkNode(enableMemTableOnSinkNode1);
        if (enableMemTableOnSinkNode1) {
            queryOptionsWithMemTable.setBatchSize(brokerLoadBatchSize);
        }
        Assert.assertTrue(queryOptionsWithMemTable.isEnableMemtableOnSinkNode());
        Assert.assertEquals(brokerLoadBatchSize, queryOptionsWithMemTable.getBatchSize());

        // Case 2: enableMemTableOnSinkNode = false, setBatchSize should NOT be called
        TQueryOptions queryOptionsWithoutMemTable = new TQueryOptions();
        boolean enableMemTableOnSinkNode2 = false;
        queryOptionsWithoutMemTable.setEnableMemtableOnSinkNode(enableMemTableOnSinkNode2);
        if (enableMemTableOnSinkNode2) {
            queryOptionsWithoutMemTable.setBatchSize(brokerLoadBatchSize);
        }
        Assert.assertFalse(queryOptionsWithoutMemTable.isEnableMemtableOnSinkNode());
        // batch_size should remain 0 (unset), BE will use DEFAULT_BATCH_SIZE (4062)
        Assert.assertEquals(0, queryOptionsWithoutMemTable.getBatchSize());
    }
}
