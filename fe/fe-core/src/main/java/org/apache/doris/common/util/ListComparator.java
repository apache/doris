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

import com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.List;

/*
 * this class is for sorting list collections
 */
public class ListComparator<T extends List<Comparable>> implements Comparator<T> {

    OrderByPair[] orderByPairs;
    boolean isDesc;
    int indicesLen;

    public ListComparator(int...indices) {
        this(false, indices);
    }

    public ListComparator(boolean isDesc, int...indices) {
        this.orderByPairs = new OrderByPair[indices.length];
        for (int i = 0; i < indices.length; i++) {
            this.orderByPairs[i] = new OrderByPair(indices[i], isDesc);
        }
        this.indicesLen = orderByPairs.length;
    }

    public ListComparator(OrderByPair...orderByPairs) {
        this.orderByPairs = orderByPairs;
        this.indicesLen = orderByPairs.length;
    }

    @Override
    public int compare(T firstList, T secondList) {
        int firstLen = firstList.size();
        int secondLen = secondList.size();
        int minLen = Math.min(firstLen, secondLen);

        OrderByPair orderByPair = null;
        for (int i = 0; i < indicesLen; ++i) {
            if (i < minLen) {
                orderByPair = orderByPairs[i];
                int ret = firstList.get(orderByPair.getIndex()).compareTo(secondList.get(orderByPair.getIndex()));
                if (ret != 0) {
                    return orderByPair.isDesc() ? -ret : ret;
                }
            }
        }

        Preconditions.checkNotNull(orderByPair);
        if (firstLen < secondLen) {
            return orderByPair.isDesc() ? 1 : -1;
        }
        if (firstLen > secondLen) {
            return orderByPair.isDesc() ? -1 : 1;
        }

        return 0;
    }
}
