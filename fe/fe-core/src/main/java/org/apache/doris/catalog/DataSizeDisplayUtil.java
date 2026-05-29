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

package org.apache.doris.catalog;

import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

public class DataSizeDisplayUtil {
    private DataSizeDisplayUtil() {
    }

    public static Pair<Long, Long> getDisplayDataSize(Replica replica) {
        return getDisplayDataSize(replica.getDataSize(), replica.getRemoteDataSize(),
                getLocalIndexAndSegmentSize(replica));
    }

    public static Pair<Long, Long> getDisplayDataSize(Partition partition) {
        long localDataSize = partition.getDataSize(false);
        long remoteDataSize = partition.getRemoteDataSize();
        if (!needCloudSizeMapping(remoteDataSize)) {
            return Pair.of(localDataSize, remoteDataSize);
        } else if (localDataSize > 0) {
            return Pair.of(0L, localDataSize);
        }
        return getDisplayDataSize(localDataSize, remoteDataSize, getPartitionLocalIndexAndSegmentSize(partition));
    }

    private static Pair<Long, Long> getDisplayDataSize(long localDataSize, long remoteDataSize,
            long localIndexAndSegmentSize) {
        if (!needCloudSizeMapping(remoteDataSize)) {
            return Pair.of(localDataSize, remoteDataSize);
        }
        if (localDataSize > 0) {
            remoteDataSize = localDataSize;
            localDataSize = 0;
        } else if (localIndexAndSegmentSize > 0) {
            remoteDataSize = localIndexAndSegmentSize;
        }
        return Pair.of(localDataSize, remoteDataSize);
    }

    private static boolean needCloudSizeMapping(long remoteDataSize) {
        return Config.isCloudMode() && remoteDataSize == 0;
    }

    private static long getPartitionLocalIndexAndSegmentSize(Partition partition) {
        long localIndexAndSegmentSize = 0L;
        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                for (Replica replica : tablet.getReplicas()) {
                    localIndexAndSegmentSize += getLocalIndexAndSegmentSize(replica);
                }
            }
        }
        return localIndexAndSegmentSize;
    }

    private static long getLocalIndexAndSegmentSize(Replica replica) {
        return replica.getLocalInvertedIndexSize() + replica.getLocalSegmentSize();
    }
}
