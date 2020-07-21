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

import java.util.ArrayList;
import java.util.List;

public class ListUtil {
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
}
