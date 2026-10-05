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

import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.DdlException;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ListUtil {

    public static final Comparator<Map.Entry<Long, PartitionItem>> LIST_MAP_ENTRY_COMPARATOR =
            Comparator.comparing(o -> ((ListPartitionItem) o.getValue()).getItems().iterator().next());
    public static final Comparator<PartitionItem> PARTITION_KEY_COMPARATOR =
            Comparator.comparing(o -> ((ListPartitionItem) o).getItems().iterator().next());

    /**
     * split a list to multi expected number sublist
     * for example:
     *
     * list is : [1, 2, 3, 4, 5, 6, 7]
     * expectedSize is : 3
     *
     * return :
     * [1, 4, 7]
     * [2, 5]
     * [3, 6]
     */
    public static  <T> List<List<T>> splitBySize(List<T> list, int expectedSize)
            throws NullPointerException, IllegalArgumentException {
        Preconditions.checkNotNull(list, "list must not be null");
        Preconditions.checkArgument(expectedSize > 0, "expectedSize must larger than 0");

        int splitSize = Math.min(expectedSize, list.size());

        List<List<T>> result = new ArrayList<List<T>>(splitSize);
        for (int i = 0; i < splitSize; i++) {
            result.add(new ArrayList<>());
        }

        int index = 0;
        for (T t : list) {
            result.get(index).add(t);
            index = (index + 1) % splitSize;
        }

        return result;
    }

    public static void checkPartitionKeyListsMatch(List<PartitionItem> list1,
            List<PartitionItem> list2) throws DdlException {
        list1.sort(PARTITION_KEY_COMPARATOR);
        list2.sort(PARTITION_KEY_COMPARATOR);

        int idx1 = 0;
        int idx2 = 0;
        List<PartitionKey> keys1 = new ArrayList<>(list1.get(idx1).getItems());
        List<PartitionKey> keys2 = new ArrayList<>(list2.get(idx2).getItems());

        while (true) {
            int size = Math.min(keys1.size(), keys2.size());
            for (int i = 0; i < size; i++) {
                int res = keys1.get(i).compareTo(keys2.get(i));
                if (res != 0) {
                    throw new DdlException("2 partition key lists are not matched. "
                            + keys1 + " vs. " + keys2);
                }
            }

            int res = keys1.size() - keys2.size();
            if (res == 0) {
                ++idx1;
                ++idx2;
                if (idx1 == list1.size() || idx2 == list2.size()) {
                    break;
                }
                keys1.clear();
                keys2.clear();
                keys1.addAll(list1.get(idx1).getItems());
                keys2.addAll(list2.get(idx2).getItems());
            } else if (res > 0) {
                if (++idx2 == list2.size()) {
                    break;
                }
                keys1.removeAll(keys2);
                keys2.clear();
                keys2.addAll(list2.get(idx2).getItems());
            } else {
                if (++idx1 == list1.size()) {
                    break;
                }
                keys2.removeAll(keys1);
                keys1.clear();
                keys1.addAll(list1.get(idx1).getItems());
            }
        }

        if (idx1 < list1.size() || idx2 < list2.size()) {
            throw new DdlException("2 partition key lists are not matched. "
                    + list1 + " vs. " + list2);
        }
    }

    // Check if items in list2 conflicts with items in list1
    public static void checkListsConflict(List<PartitionItem> list1, List<PartitionItem> list2) throws DdlException {
        for (PartitionItem checkItem : list2) {
            List<PartitionKey> checkKeys = checkItem.getItems();
            for (PartitionKey checkKey : checkKeys) {
                for (PartitionItem currentItem : list1) {
                    if (((ListPartitionItem) currentItem).getItems().contains(checkKey)) {
                        throw new DdlException("The partition key[" + checkKey.toSql()
                                + "] is overlap with current " + currentItem);
                    }
                }
            }
        }
    }
}
