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

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DorisPartitionFactory {

    public static DorisPartitioner create(
            EtlJobConfig.EtlPartitionInfo partitionInfo,
            List<EtlColumn> columns,
            List<String> keyAndPartitionColumnNames) throws SparkDppException {

        switch (partitionInfo.partitionType.toUpperCase(Locale.ROOT)) {
            case "RANGE":
                return DorisRangePartitioner.build(partitionInfo, columns, keyAndPartitionColumnNames);
            case "UNPARTITIONED":
                return new DorisUnPartitioner();
            case "LIST":
                // TODO implement
            default:
                throw new SparkDppException("not supported partition type: " + partitionInfo.partitionType);
        }
    }

    public static DorisPartitioner create(Map<String, Integer> bucketKeyMap) {
        return new DorisBucketPartitioner(bucketKeyMap);
    }
}
