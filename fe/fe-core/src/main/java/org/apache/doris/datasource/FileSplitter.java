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

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileCompressType;

import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class FileSplitter {
    private static final Logger LOG = LogManager.getLogger(FileSplitter.class);

    // If the number of files is larger than parallel instances * num of backends,
    // we don't need to split the file.
    // Otherwise, split the file to avoid local shuffle.
    public static boolean needSplitForCountPushdown(int parallelism, int numBackends, long totalFileNum) {
        return totalFileNum < parallelism * numBackends;
    }

    public static List<Split> splitFile(
            LocationPath path,
            long fileSplitSize,
            BlockLocation[] blockLocations,
            long length,
            long modificationTime,
            boolean splittable,
            List<String> partitionValues,
            SplitCreator splitCreator)
            throws IOException {
        if (blockLocations == null) {
            blockLocations = new BlockLocation[0];
        }
        List<Split> result = Lists.newArrayList();
        TFileCompressType compressType = Util.inferFileCompressTypeByPath(path.get());
        if (!splittable || compressType != TFileCompressType.PLAIN) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Path {} is not splittable.", path);
            }
            String[] hosts = blockLocations.length == 0 ? null : blockLocations[0].getHosts();
            result.add(splitCreator.create(path, 0, length, length, fileSplitSize,
                    modificationTime, hosts, partitionValues));
            return result;
        }
        long bytesRemaining;
        for (bytesRemaining = length; (double) bytesRemaining / (double) fileSplitSize > 1.1D;
                bytesRemaining -= fileSplitSize) {
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
            result.add(splitCreator.create(path, length - bytesRemaining, fileSplitSize,
                    length, fileSplitSize, modificationTime, hosts, partitionValues));
        }
        if (bytesRemaining != 0L) {
            int location = getBlockIndex(blockLocations, length - bytesRemaining);
            String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
            result.add(splitCreator.create(path, length - bytesRemaining, bytesRemaining,
                    length, fileSplitSize, modificationTime, hosts, partitionValues));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Path {} includes {} splits.", path, result.size());
        }
        return result;
    }

    private static int getBlockIndex(BlockLocation[] blkLocations, long offset) {
        if (blkLocations == null || blkLocations.length == 0) {
            return -1;
        }
        for (int i = 0; i < blkLocations.length; ++i) {
            if (blkLocations[i].getOffset() <= offset
                    && offset < blkLocations[i].getOffset() + blkLocations[i].getLength()) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1L;
        throw new IllegalArgumentException(String.format("Offset %d is outside of file (0..%d)", offset, fileLength));
    }


}
