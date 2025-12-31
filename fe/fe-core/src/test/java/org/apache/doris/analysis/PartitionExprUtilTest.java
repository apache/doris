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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.AutoBucketUtils;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendServiceImpl;
import org.apache.doris.thrift.TCreatePartitionRequest;
import org.apache.doris.thrift.TCreatePartitionResult;
import org.apache.doris.thrift.TNullableStringLiteral;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class PartitionExprUtilTest extends TestWithFeService {

    ExecuteEnv exeEnv;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.autobucket_max_buckets = 10000;
        createDatabase("test");
        exeEnv = ExecuteEnv.getInstance();
    }

    @Test
    public void testAutoBucketLargeDataCalculatesBuckets() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.auto_bucket_calc_large (\n"
                + "  event_day DATETIME NOT NULL,\n"
                + "  site_id INT,\n"
                + "  v INT\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id)\n"
                + "AUTO PARTITION BY range (date_trunc(event_day,'day')) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day) BUCKETS AUTO\n"
                + "PROPERTIES(\"replication_num\"=\"1\");";

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("auto_bucket_calc_large");

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

        // Create 3 historical partitions: 2023-08-01, 02, 03
        String[] days = {"2023-08-01 00:00:00", "2023-08-02 00:00:00", "2023-08-03 00:00:00"};
        String[] partNames = {"p20230801000000", "p20230802000000", "p20230803000000"};
        for (String day : days) {
            List<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
            List<TNullableStringLiteral> values = new ArrayList<>();
            TNullableStringLiteral start = new TNullableStringLiteral();
            start.setValue(day);
            values.add(start);
            partitionValues.add(values);

            TCreatePartitionRequest request = new TCreatePartitionRequest();
            request.setDbId(db.getId());
            request.setTableId(table.getId());
            request.setPartitionValues(partitionValues);
            TCreatePartitionResult result = impl.createPartition(request);
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
        }

        // Mark them as having data (visibleVersion >= 2)
        table.writeLockOrDdlException();
        try {
            for (String pn : partNames) {
                Partition p = table.getPartition(pn);
                Assertions.assertNotNull(p);
                p.setVisibleVersionAndTime(2L, System.currentTimeMillis());
            }
        } finally {
            table.writeUnlock();
        }

        // Mock large compressed data sizes for each historical partition
        final long GB = 1024L * 1024L * 1024L;
        Partition p1 = table.getPartition(partNames[0]);
        Partition p2 = table.getPartition(partNames[1]);
        Partition p3 = table.getPartition(partNames[2]);
        new Expectations(p1, p2, p3) {
            {
                p1.getDataSizeExcludeEmptyReplica(true);
                result = 1000 * GB; // ~1 TB uncompressed
                minTimes = 0;
                p2.getDataSizeExcludeEmptyReplica(true);
                result = 1500 * GB; // ~1.5 TB uncompressed
                minTimes = 0;
                p3.getDataSizeExcludeEmptyReplica(true);
                result = 2000 * GB; // ~2 TB uncompressed
                minTimes = 0;
            }
        };

        // Directly mock BE disk cap to a very large value so AutoBucketUtils doesn't limit by disks
        new MockUp<AutoBucketUtils>() {
            @Mock
            public int getBucketsNumByBEDisks() { // private static in target, allowed to mock by name
                return Integer.MAX_VALUE;
            }
        };

        // Compute expected bucket by reusing scheduler helpers before creating the new partition (2023-08-04)
        String newPartName = "p20230804000000";

        // Create the new partition (2023-08-04) and verify bucket num
        List<List<TNullableStringLiteral>> partitionValues4 = new ArrayList<>();
        List<TNullableStringLiteral> values4 = new ArrayList<>();
        TNullableStringLiteral start4 = new TNullableStringLiteral();
        start4.setValue("2023-08-04 00:00:00");
        values4.add(start4);
        partitionValues4.add(values4);

        TCreatePartitionRequest request4 = new TCreatePartitionRequest();
        request4.setDbId(db.getId());
        request4.setTableId(table.getId());
        request4.setPartitionValues(partitionValues4);
        TCreatePartitionResult result4 = impl.createPartition(request4);
        Assertions.assertEquals(TStatusCode.OK, result4.getStatus().getStatusCode());

        Partition p4 = table.getPartition(newPartName);
        Assertions.assertNotNull(p4);
        Assertions.assertEquals(p4.getDistributionInfo().getBucketNum(), 500);
    }

    @Test
    public void testAutoBucketReusePrevPartitionBuckets() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.auto_bucket_calc (\n"
                + "  event_day DATETIME NOT NULL,\n"
                + "  site_id INT,\n"
                + "  v INT\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id)\n"
                + "AUTO PARTITION BY range (date_trunc(event_day,'day')) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day) BUCKETS AUTO\n"
                + "PROPERTIES(\"replication_num\"=\"1\");";

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("auto_bucket_calc");

        // Create first partition via RPC (acts as historical partition)
        List<List<TNullableStringLiteral>> partitionValues = new ArrayList<>();
        List<TNullableStringLiteral> values = new ArrayList<>();
        TNullableStringLiteral start = new TNullableStringLiteral();
        start.setValue("2023-08-05 00:00:00");
        values.add(start);
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult result = impl.createPartition(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());

        Partition p1 = table.getPartition("p20230805000000");
        Assertions.assertNotNull(p1);
        // Mark the partition as having data (visibleVersion >= 2) to be counted by filterDataPartitions
        table.writeLockOrDdlException();
        try {
            p1.setVisibleVersionAndTime(2L, System.currentTimeMillis());
        } finally {
            table.writeUnlock();
        }
        // Create next partition; PartitionExprUtil should reuse previous partition buckets (8)
        List<List<TNullableStringLiteral>> partitionValues2 = new ArrayList<>();
        List<TNullableStringLiteral> values2 = new ArrayList<>();
        TNullableStringLiteral start2 = new TNullableStringLiteral();
        start2.setValue("2023-08-06 00:00:00");
        values2.add(start2);
        partitionValues2.add(values2);

        TCreatePartitionRequest request2 = new TCreatePartitionRequest();
        request2.setDbId(db.getId());
        request2.setTableId(table.getId());
        request2.setPartitionValues(partitionValues2);
        TCreatePartitionResult result2 = impl.createPartition(request2);
        Assertions.assertEquals(TStatusCode.OK, result2.getStatus().getStatusCode());

        Partition p2 = table.getPartition("p20230806000000");
        Assertions.assertNotNull(p2);
        Assertions.assertEquals(p1.getDistributionInfo().getBucketNum(), p2.getDistributionInfo().getBucketNum());
    }
}
