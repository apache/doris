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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.paimon.PaimonPartition;
import org.apache.doris.datasource.paimon.PaimonPartitionInfo;
import org.apache.doris.datasource.paimon.PaimonUtil;

import com.google.common.collect.Lists;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PaimonUtilTest {

    @Test
    public void testGeneratePartitionInfo() throws AnalysisException {
        Column k1 = new Column("k1", PrimitiveType.INT);
        Column k2 = new Column("k2", PrimitiveType.VARCHAR);
        List<Column> partitionColumns = Lists.newArrayList(k1, k2);
        PaimonPartition p1 = new PaimonPartition("[1,aa]", 2, 3, 4, 5);
        List<PaimonPartition> paimonPartitions = Lists.newArrayList(p1);
        PaimonPartitionInfo partitionInfo = PaimonUtil.generatePartitionInfo(partitionColumns, paimonPartitions);
        String expectPartitionName = "k1=1/k2=aa";
        Assert.assertTrue(partitionInfo.getNameToPartitionItem().containsKey(expectPartitionName));
        PartitionItem partitionItem = partitionInfo.getNameToPartitionItem().get(expectPartitionName);
        List<PartitionKey> keys = partitionItem.getItems();
        Assert.assertEquals(1, keys.size());
        PartitionKey partitionKey = keys.get(0);
        List<LiteralExpr> exprs = partitionKey.getKeys();
        Assert.assertEquals(2, exprs.size());
        Assert.assertEquals(1, exprs.get(0).getLongValue());
        Assert.assertEquals("aa", exprs.get(1).getStringValue());
    }

    @Test
    public void testRowToPartition() {
        GenericRow row = GenericRow.of(BinaryString.fromString("[1,b]"), 2L, 3L, 4L, Timestamp.fromEpochMillis(5L));
        PaimonPartition paimonPartition = PaimonUtil.rowToPartition(row);
        Assert.assertEquals("[1,b]", paimonPartition.getPartitionValues());
        Assert.assertEquals(2L, paimonPartition.getRecordCount());
        Assert.assertEquals(3L, paimonPartition.getFileSizeInBytes());
        Assert.assertEquals(4L, paimonPartition.getFileCount());
        Assert.assertEquals(5L, paimonPartition.getLastUpdateTime());
    }
}
