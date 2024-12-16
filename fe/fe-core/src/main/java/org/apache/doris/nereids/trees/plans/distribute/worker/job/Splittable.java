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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/** Splittable */
public interface Splittable<S extends Splittable<S>> {

    int itemSize();

    void addItem(S other, int index);

    S newSplittable();

    default List<S> split(int splitSize) {
        return Splittable.split(this, splitSize);
    }

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
    static <S extends Splittable<S>> List<S> split(Splittable<S> splittable, int splitSize) {
        Preconditions.checkNotNull(splittable, "splittable must not be null");
        Preconditions.checkArgument(splitSize > 0, "splitSize must larger than 0");

        int itemSize = splittable.itemSize();
        splitSize = Math.min(splitSize, itemSize);

        List<S> result = new ArrayList<>(splitSize);
        for (int i = 0; i < splitSize; i++) {
            result.add(splittable.newSplittable());
        }

        int index = 0;
        for (int i = 0; i < itemSize; i++) {
            result.get(index).addItem((S) splittable, i);
            index = (index + 1) % splitSize;
        }
        return result;
    }
}
