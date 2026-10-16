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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSplitter {
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
        remainingInitialSplitNum = new AtomicInteger(maxInitialSplitNum);
    }

    public List<Split> splitFile(LocationPath path, long specifiedFileSplitSize,
            BlockLocation[] blockLocations, long length, long modificationTime, boolean splittable,
            List<String> partitionValues, SplitCreator splitCreator) throws IOException {
        List<Split> result = Lists.newArrayList();
        try {
            splitFileIterator(path, specifiedFileSplitSize, blockLocations, length, modificationTime,
                    splittable, partitionValues, splitCreator).forEachRemaining(result::add);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        return result;
    }

    /** Uses the existing file boundaries while constructing only ranges actually consumed. */
    public Iterator<Split> splitFileIterator(LocationPath path, long specifiedFileSplitSize,
            BlockLocation[] blockLocations, long length, long modificationTime, boolean splittable,
            List<String> partitionValues, SplitCreator splitCreator) throws IOException {
        if (length <= 0) {
            return Collections.emptyIterator();
        }
        long targetFileSplitSize = specifiedFileSplitSize > 0 ? specifiedFileSplitSize : maxSplitSize;
        BlockLocation[] locations = blockLocations == null
                ? new BlockLocation[] {new BlockLocation(null, null, 0L, length)} : blockLocations;
        boolean canSplit = splittable
                && Util.inferFileCompressTypeByPath(path.getNormalizedLocation()) == TFileCompressType.PLAIN;
        ImmutableList.Builder<InternalBlock> blockBuilder = ImmutableList.builder();
        if (canSplit && specifiedFileSplitSize <= 0) {
            for (BlockLocation block : locations) {
                long blockStart = Math.max(0, block.getOffset());
                long blockEnd = Math.min(length, block.getOffset() + block.getLength());
                if (blockStart > blockEnd || blockStart == blockEnd && length > 0) {
                    continue;
                }
                blockBuilder.add(new InternalBlock(blockStart, blockEnd, block.getHosts()));
            }
        }
        List<InternalBlock> blocks = blockBuilder.build();
        return new Iterator<Split>() {
            private long offset;
            private int blockIndex;

            @Override
            public boolean hasNext() {
                return offset < length;
            }

            @Override
            public Split next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                try {
                    long bytes;
                    String[] hosts;
                    if (!canSplit) {
                        bytes = length;
                        hosts = locations.length == 0 ? null : locations[0].getHosts();
                        nextMaxSplitSize();
                    } else if (specifiedFileSplitSize > 0) {
                        long remaining = length - offset;
                        bytes = (double) remaining / specifiedFileSplitSize > 1.1D
                                ? specifiedFileSplitSize : remaining;
                        int index = getBlockIndex(locations, offset);
                        hosts = index == -1 ? null : locations[index].getHosts();
                    } else if (blocks.isEmpty()) {
                        bytes = length;
                        hosts = null;
                        nextMaxSplitSize();
                    } else {
                        long maxSize = nextMaxSplitSize();
                        InternalBlock block = blocks.get(blockIndex);
                        long remaining = block.getEnd() - offset;
                        bytes = remaining <= maxSize ? remaining
                                : maxSize * 2 >= remaining ? remaining / 2 : maxSize;
                        hosts = block.getHosts();
                    }
                    Split result = splitCreator.create(path, offset, bytes, length, targetFileSplitSize,
                            modificationTime, hosts, partitionValues);
                    offset += bytes;
                    if (canSplit && specifiedFileSplitSize <= 0 && !blocks.isEmpty()
                            && offset == blocks.get(blockIndex).getEnd()) {
                        blockIndex++;
                        if (blockIndex < blocks.size()) {
                            Verify.verify(offset == blocks.get(blockIndex).getStart());
                        }
                    }
                    return result;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private long nextMaxSplitSize() {
        int cur = remainingInitialSplitNum.get();
        while (cur > 0) {
            if (remainingInitialSplitNum.compareAndSet(cur, cur - 1)) {
                return maxInitialSplitSize;
            }
            cur = remainingInitialSplitNum.get();
        }
        return maxSplitSize;
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
