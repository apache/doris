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

package org.apache.doris.job.extensions.insert;

import org.apache.commons.lang3.Range;

import java.util.ArrayList;
import java.util.List;

/**
 * SQLRangeGenerator is responsible for generating sharded SQL ranges based on the given parameters.
 * This can be used to divide a SQL query into multiple ranges for parallel processing.
 * <p>
 * Example:
 * If originalSQL = "SELECT * FROM table WHERE age = 1", shardKey = "id",
 * shardCount = 2, lowerLimit = 0, upperLimit = 100,
 * the generated ranges will be: ["SELECT * FROM table WHERE age = 1 AND id >= 0 AND id <= 50",
 * "SELECT * FROM table WHERE age = 1 AND id > 50 AND id <= 100"]
 * Ranges is [0,50], [51, 100]
 */
public class SQLRangeGenerator {
    /**
     * Generates a list of ranges based on the specified shard count,
     * dividing the interval between lowerLimit and upperLimit inclusively.
     * Adjusts for cases where the range is smaller than the shard count,
     * ensuring each individual value gets its own range if necessary.
     *
     * @param shardCount The desired number of ranges to generate.
     * @param lowerLimit The lower inclusive bound of the range.
     * @param upperLimit The upper inclusive bound of the range.
     * @return A list of Range objects representing the divided segments.
     */
    public static List<Range> generateRanges(int shardCount, long lowerLimit, long upperLimit) {
        List<Range> ranges = new ArrayList<>();

        // Adjust logic to handle cases where the range is smaller or equal to shardCount
        if (upperLimit - lowerLimit + 1 <= shardCount) {
            for (long i = lowerLimit; i <= upperLimit; i++) {
                ranges.add(Range.between(i, i));
            }
            return ranges;
        }
        long rangeSize = (upperLimit - lowerLimit + 1) / shardCount;
        for (int i = 0; i < shardCount; i++) {
            long start = lowerLimit + i * rangeSize;
            // Ensure the last range includes upperLimit if it's not perfectly divisible
            long end = (i == shardCount - 1) ? upperLimit : start + rangeSize - 1;
            ranges.add(Range.between(start, end));
        }
        return ranges;
    }

}
