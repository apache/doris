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

import org.apache.doris.thrift.TConnectorFileCommitData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class LoadContextTest {
    @Test
    void deduplicatesConnectorFileCommitDataByPath() {
        LoadContext context = new LoadContext();
        TConnectorFileCommitData original = new TConnectorFileCommitData()
                .setFilePath("s3://bucket/table/part-0.parquet")
                .setRowCount(1);
        TConnectorFileCommitData duplicate = new TConnectorFileCommitData()
                .setFilePath("s3://bucket/table/part-0.parquet")
                .setRowCount(2);
        TConnectorFileCommitData second = new TConnectorFileCommitData()
                .setFilePath("s3://bucket/table/part-1.parquet")
                .setRowCount(3);

        context.updateConnectorFileCommitDatas(Arrays.asList(original, duplicate, second));

        List<TConnectorFileCommitData> results = context.getConnectorFileCommitDatas();
        Assertions.assertEquals(2, results.size());
        Assertions.assertEquals(2, results.get(0).getRowCount());
        Assertions.assertEquals(3, results.get(1).getRowCount());
    }
}
