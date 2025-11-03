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

package org.apache.doris.common.util;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.DdlException;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class RangeUtils {

    public static final Comparator<Map.Entry<Long, PartitionItem>> RANGE_MAP_ENTRY_COMPARATOR =
            Comparator.comparing(o -> (((RangePartitionItem) o.getValue()).getItems()).lowerEndpoint());

    public static final Comparator<PartitionItem> RANGE_COMPARATOR =
            Comparator.comparing(o -> ((RangePartitionItem) o).getItems().lowerEndpoint());

    public static void checkRangeIntersect(Range<PartitionKey> range1, Range<PartitionKey> range2) throws DdlException {
        if (range2.isConnected(range1)) {
            if (!range2.intersection(range1).isEmpty()) {
                throw new DdlException("Range " + range1 + " is intersected with range: " + range2);
            }
        }
    }

    public static boolean checkIsTwoRangesIntersect(Range<PartitionKey> range1, Range<PartitionKey> range2) {
        if (range2.isConnected(range1) && !range2.intersection(range1).isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Pass only if the 2 range lists are exactly same
     * What is "exactly same"?
     *      1. {[0, 10), [10, 20)} exactly same as {[0, 20)}
     *      2. {[0, 10), [15, 20)} exactly same as {[0, 10), [15, 18), [18, 20)}
     *      3. {[0, 10), [15, 20)} exactly same as {[0, 10), [15, 20)}
     *      4. {[0, 10), [15, 20)} NOT exactly same as {[0, 20)}
     *
     *  Here I will use an example to explain the algorithm:
     *      list1: {[0, 10), [15, 20)}
     *      list2: {[0, 10), [15, 18), [18, 20)}
     *
     *  1. sort 2 lists first (the above 2 lists are already sorted)
     *  2. Begin to compare ranges from index 0: [0, 10) and [0, 10)
     *      2.1 lower bounds (0 and 0) are equal.
     *      2.2 upper bounds (10 and 10) are equal.
     *  3. Begin to compare next 2 ranges [15, 20) and [15, 18)
     *      3.1 lower bounds (15 and 15) are equal.
     *      3.2 upper bounds (20 and 18) are not equal. and 20 > 18
     *      3.3 Split range [15, 20) to [15, 18) and [18, 20)
     *  4. Begin to compare next 2 ranges [18, 20) and [18, 20), the first [18, 20) is the splitted range
     *      4.1 lower bounds (18 and 18) are equal.
     *      4.2 upper bounds (20 and 20) are equal.
     *  5. Not more next ranges, so 2 lists are equal.
     */
    public static void checkPartitionItemListsMatch(List<PartitionItem> list1, List<PartitionItem> list2)
            throws DdlException {
        Collections.sort(list1, RangeUtils.RANGE_COMPARATOR);
        Collections.sort(list2, RangeUtils.RANGE_COMPARATOR);

        int idx1 = 0;
        int idx2 = 0;
        Range<PartitionKey> range1 = list1.get(idx1).getItems();
        Range<PartitionKey> range2 = list2.get(idx2).getItems();
        while (true) {
            if (range1.lowerEndpoint().compareTo(range2.lowerEndpoint()) != 0) {
                throw new DdlException("2 range lists are not stricly matched. "
                        + range1.lowerEndpoint() + " vs. " + range2.lowerEndpoint());
            }

            int res = range1.upperEndpoint().compareTo(range2.upperEndpoint());
            if (res == 0) {
                ++idx1;
                ++idx2;
                if (idx1 == list1.size() || idx2 == list2.size()) {
                    break;
                }
                range1 = list1.get(idx1).getItems();
                range2 = list2.get(idx2).getItems();
                continue;
            } else if (res > 0) {
                if (++idx2 == list2.size()) {
                    break;
                }
                range1 = Range.closedOpen(range2.upperEndpoint(), range1.upperEndpoint());
                range2 = list2.get(idx2).getItems();
            } else {
                if (++idx1 == list1.size()) {
                    break;
                }
                range2 = Range.closedOpen(range1.upperEndpoint(), range2.upperEndpoint());
                range1 = list1.get(idx1).getItems();
            }
        }

        if (idx1 < list1.size() || idx2 < list2.size()) {
            throw new DdlException("2 range lists are not stricly matched. "
                    + list1 + " vs. " + list2);
        }
    }

    public static Range<PartitionKey> readRange(DataInput in) throws IOException {
        boolean hasLowerBound = false;
        boolean hasUpperBound = false;
        boolean lowerBoundClosed = false;
        boolean upperBoundClosed = false;
        PartitionKey lowerBound = null;
        PartitionKey upperBound = null;

        hasLowerBound = in.readBoolean();
        if (hasLowerBound) {
            lowerBoundClosed = in.readBoolean();
            lowerBound = PartitionKey.read(in);
        }

        hasUpperBound = in.readBoolean();
        if (hasUpperBound) {
            upperBoundClosed = in.readBoolean();
            upperBound = PartitionKey.read(in);
        }

        // Totally 9 cases. Both lower bound and upper bound could be open, closed or not exist
        if (hasLowerBound && lowerBoundClosed && hasUpperBound && upperBoundClosed) {
            return Range.closed(lowerBound, upperBound);
        }
        if (hasLowerBound && lowerBoundClosed && hasUpperBound && !upperBoundClosed) {
            return Range.closedOpen(lowerBound, upperBound);
        }
        if (hasLowerBound && !lowerBoundClosed && hasUpperBound && upperBoundClosed) {
            return Range.openClosed(lowerBound, upperBound);
        }
        if (hasLowerBound && !lowerBoundClosed && hasUpperBound && !upperBoundClosed) {
            return Range.open(lowerBound, upperBound);
        }
        if (hasLowerBound && lowerBoundClosed && !hasUpperBound) {
            return Range.atLeast(lowerBound);
        }
        if (hasLowerBound && !lowerBoundClosed && !hasUpperBound) {
            return Range.greaterThan(lowerBound);
        }
        if (!hasLowerBound && hasUpperBound && upperBoundClosed) {
            return Range.atMost(upperBound);
        }
        if (!hasLowerBound && hasUpperBound && !upperBoundClosed) {
            return Range.lessThan(upperBound);
        }
        // Neither lower bound nor upper bound exists, return null. This means just one partition
        return null;
    }

    // check if any ranges in "rangesToBeChecked" conflict with ranges in "baseRanges".
    public static void checkRangeConflict(List<PartitionItem> baseRanges,
            List<PartitionItem> rangesToBeChecked) throws DdlException {

        RangeMap<PartitionKey, Long> baseRangeMap = TreeRangeMap.create();
        long idx = 0;
        for (PartitionItem item : baseRanges) {
            baseRangeMap.put(item.getItems(), idx++);
        }

        for (PartitionItem item : rangesToBeChecked) {
            if (!baseRangeMap.subRangeMap(item.getItems()).asMapOfRanges().isEmpty()) {
                throw new DdlException("Range: " + item.getItems() + " conflicts with existing range");
            }
        }
    }

    public static class RangeSerializer implements
                JsonSerializer<Range<PartitionKey>>, JsonDeserializer<Range<PartitionKey>> {
        @Override
        public JsonElement serialize(Range<PartitionKey> range, Type type, JsonSerializationContext context) {
            JsonArray result = new JsonArray();

            // write lower bound if lower bound exists
            if (range.hasLowerBound()) {
                PartitionKey lowerBound = range.lowerEndpoint();
                JsonObject lowerBoundObject = new JsonObject();
                lowerBoundObject.addProperty("type", range.lowerBoundType().toString());
                lowerBoundObject.add("value", context.serialize(lowerBound));
                result.add(lowerBoundObject);
            } else {
                result.add(JsonNull.INSTANCE);
            }

            // write upper bound if upper bound exists
            if (range.hasUpperBound()) {
                PartitionKey upperBound = range.upperEndpoint();
                JsonObject upperBoundObject = new JsonObject();
                upperBoundObject.addProperty("type", range.upperBoundType().toString());
                upperBoundObject.add("value", context.serialize(upperBound));
                result.add(upperBoundObject);
            } else {
                result.add(JsonNull.INSTANCE);
            }

            return result;
        }

        @Override
        public Range<PartitionKey> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
            JsonArray jsonArray = json.getAsJsonArray();
            PartitionKey lowerBound = null;
            BoundType lowerBoundType = BoundType.CLOSED;
            PartitionKey upperBound = null;
            BoundType upperBoundType = BoundType.CLOSED;

            if (!jsonArray.get(0).isJsonNull()) {
                JsonObject lowerBoundObject = jsonArray.get(0).getAsJsonObject();
                lowerBoundType = BoundType.valueOf(lowerBoundObject.get("type").getAsString());
                lowerBound = context.deserialize(lowerBoundObject.get("value"), PartitionKey.class);
            }

            if (!jsonArray.get(1).isJsonNull()) {
                JsonObject upperBoundObject = jsonArray.get(1).getAsJsonObject();
                upperBoundType = BoundType.valueOf(upperBoundObject.get("type").getAsString());
                upperBound = context.deserialize(upperBoundObject.get("value"), PartitionKey.class);
            }

            if (lowerBound == null && upperBound == null) {
                return null;
            }
            if (lowerBound == null) {
                return Range.upTo(upperBound, upperBoundType);
            }
            if (upperBound == null) {
                return Range.downTo(lowerBound, lowerBoundType);
            }
            return Range.range(lowerBound, lowerBoundType, upperBound, upperBoundType);
        }
    }
}
