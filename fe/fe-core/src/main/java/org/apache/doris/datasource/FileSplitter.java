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

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSplitter {
    private static final Logger LOG = LogManager.getLogger(FileSplitter.class);

    // If the number of files is larger than parallel instances * num of backends,
    // we don't need to split the file.
    // Otherwise, split the file to avoid local shuffle.
    public static boolean needSplitForCountPushdown(int parallelism, int numBackends, long totalFileNum) {
        return totalFileNum < parallelism * numBackends;
    }

    private long maxInitialSplitSize;

    private long maxSplitSize;

    private int maxInitialSplitNum;
    private final AtomicInteger remainingInitialSplitNum;

    private long currentMaxSplitSize;

    public long getMaxInitialSplitSize() {
        return maxInitialSplitSize;
    }

    public void setMaxInitialSplitSize(long maxInitialSplitSize) {
        this.maxInitialSplitSize = maxInitialSplitSize;
    }

    public long getMaxSplitSize() {
        return maxSplitSize;
    }

    public void setMaxSplitSize(long maxSplitSize) {
        this.maxSplitSize = maxSplitSize;
    }

    public int maxInitialSplitNum() {
        return maxInitialSplitNum;
    }

    public void setMaxInitialSplits(int maxInitialSplitNum) {
        this.maxInitialSplitNum = maxInitialSplitNum;
    }

    public long getRemainingInitialSplitNum() {
        return remainingInitialSplitNum.get();
    }

    public FileSplitter(long maxInitialSplitSize, long maxSplitSize, int maxInitialSplitNum) {
        this.maxInitialSplitSize = maxInitialSplitSize;
        this.maxSplitSize = maxSplitSize;
        this.maxInitialSplitNum = maxInitialSplitNum;
        currentMaxSplitSize = maxInitialSplitSize;
        remainingInitialSplitNum = new AtomicInteger(maxInitialSplitNum);
    }

    public List<Split> splitFile(
                LocationPath path,
                long specifiedFileSplitSize,
                BlockLocation[] blockLocations,
                long length,
                long modificationTime,
                boolean splittable,
                List<String> partitionValues,
                SplitCreator splitCreator)
                throws IOException {
        // Pass splitCreator.create() to set target file split size to calculate split weight.
        long targetFileSplitSize = specifiedFileSplitSize > 0 ? specifiedFileSplitSize : maxSplitSize;
        if (blockLocations == null) {
            blockLocations = new BlockLocation[1];
            blockLocations[0] = new BlockLocation(null, null, 0L, length);
        }
        List<Split> result = Lists.newArrayList();
        TFileCompressType compressType = Util.inferFileCompressTypeByPath(path.getNormalizedLocation());
        if (!splittable || compressType != TFileCompressType.PLAIN) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Path {} is not splittable.", path);
            }
            String[] hosts = blockLocations.length == 0 ? null : blockLocations[0].getHosts();
            result.add(splitCreator.create(path, 0, length, length,
                    targetFileSplitSize,
                    modificationTime, hosts, partitionValues));
            updateCurrentMaxSplitSize();
            return result;
        }

        // if specified split size is not zero, split file by specified size
        if (specifiedFileSplitSize > 0) {
            long bytesRemaining;
            for (bytesRemaining = length; (double) bytesRemaining / (double) specifiedFileSplitSize > 1.1D;
                    bytesRemaining -= specifiedFileSplitSize) {
                int location = getBlockIndex(blockLocations, length - bytesRemaining);
                String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
                result.add(splitCreator.create(path, length - bytesRemaining, specifiedFileSplitSize,
                        length, specifiedFileSplitSize, modificationTime, hosts, partitionValues));
            }
            if (bytesRemaining != 0L) {
                int location = getBlockIndex(blockLocations, length - bytesRemaining);
                String[] hosts = location == -1 ? null : blockLocations[location].getHosts();
                result.add(splitCreator.create(path, length - bytesRemaining, bytesRemaining,
                        length, specifiedFileSplitSize, modificationTime, hosts, partitionValues));
            }
            return result;
        }

        // split file by block
        long start = 0;
        ImmutableList.Builder<InternalBlock> blockBuilder = ImmutableList.builder();
        for (BlockLocation blockLocation : blockLocations) {
            // clamp the block range
            long blockStart = Math.max(start, blockLocation.getOffset());
            long blockEnd = Math.min(start + length, blockLocation.getOffset() + blockLocation.getLength());
            if (blockStart > blockEnd) {
                // block is outside split range
                continue;
            }
            if (blockStart == blockEnd && !(blockStart == start && blockEnd == start + length)) {
                // skip zero-width block, except in the special circumstance:
                // slice is empty, and the block covers the empty slice interval.
                continue;
            }
            blockBuilder.add(new InternalBlock(blockStart, blockEnd, blockLocation.getHosts()));
        }
        List<InternalBlock> blocks = blockBuilder.build();
        if (blocks.isEmpty()) {
            result.add(splitCreator.create(path, 0, length, length,
                    targetFileSplitSize, modificationTime, null,
                    partitionValues));
            updateCurrentMaxSplitSize();
            return result;
        }

        long splitStart = start;
        int currentBlockIdx = 0;
        while (splitStart < start + length) {
            updateCurrentMaxSplitSize();
            long splitBytes;
            long remainingBlockBytes = blocks.get(currentBlockIdx).getEnd() - splitStart;
            if (remainingBlockBytes <= currentMaxSplitSize) {
                splitBytes = remainingBlockBytes;
            } else if (currentMaxSplitSize * 2 >= remainingBlockBytes) {
                // Second to last split in this block, generate two evenly sized splits
                splitBytes = remainingBlockBytes / 2;
            } else {
                splitBytes = currentMaxSplitSize;
            }
            result.add(splitCreator.create(path, splitStart, splitBytes,
                    length, targetFileSplitSize, modificationTime, blocks.get(currentBlockIdx).getHosts(),
                    partitionValues));
            splitStart += splitBytes;
            if (splitStart == blocks.get(currentBlockIdx).getEnd()) {
                currentBlockIdx++;
                if (currentBlockIdx != blocks.size()) {
                    Verify.verify(splitStart == blocks.get(currentBlockIdx).getStart());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Path {} includes {} splits.", path, result.size());
        }
        return result;
    }

    private void updateCurrentMaxSplitSize() {
        currentMaxSplitSize = maxSplitSize;
        int cur = remainingInitialSplitNum.get();
        while (cur > 0) {
            if (remainingInitialSplitNum.compareAndSet(cur, cur - 1)) {
                currentMaxSplitSize = maxInitialSplitSize;
                break;
            }
            cur = remainingInitialSplitNum.get();
        }
    }

    private int getBlockIndex(BlockLocation[] blkLocations, long offset) {
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

    private static class InternalBlock {
        private final long start;
        private final long end;
        private final String[] hosts;

        public InternalBlock(long start, long end, String[] hosts) {
            Preconditions.checkArgument(start <= end, "block end cannot be before block start");
            this.start = start;
            this.end = end;
            this.hosts = hosts;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public String[] getHosts() {
            return hosts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InternalBlock that = (InternalBlock) o;
            return start == that.start && end == that.end && Arrays.equals(hosts, that.hosts);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(start, end);
            result = 31 * result + Arrays.hashCode(hosts);
            return result;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("InternalBlock{");
            sb.append("start=").append(start);
            sb.append(", end=").append(end);
            sb.append(", hosts=").append(Arrays.toString(hosts));
            sb.append('}');
            return sb.toString();
        }
    }


}
