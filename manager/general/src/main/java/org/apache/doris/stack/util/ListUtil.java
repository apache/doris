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

package org.apache.doris.stack.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description：list Element processing tool class
 */
public class ListUtil {

    private ListUtil() {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param newList
     * @param oldList
     * @return Returns a list of elements that have always existed
     * @Description:
     * Calculate the reduction of the new list NEWLIST relative to the old list oldlist,
     * and be compatible with the list data structure of any type of element
     */
    public static <E> List<E> getExistList(List<E> newList, List<E> oldList) {
        List<E> existList = new ArrayList<>();
        if (newList == null != newList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return existList;
        }
        for (int i = 0; i < newList.size(); i++) {
            if (listContains(oldList, newList.get(i))) {
                existList.add(newList.get(i));
            }
        }
        return existList;
    }

    /**
     *
     * @param newList
     * @param oldList
     * @return Returns a list of reduced elements
     * @Description:
     * Calculate the reduction of the new list NEWLIST relative to the old list oldlist,
     * and be compatible with the list data structure of any type of element
     */
    public static <E> List<E> getReduceList(List<E> newList, List<E> oldList) {
        if (newList == null || newList.isEmpty()) {
            return oldList;
        }
        List<E> reduceaList = new ArrayList<>();
        for (int i = 0; i < oldList.size(); i++) {
            if (!listContains(newList, oldList.get(i))) {
                reduceaList.add(oldList.get(i));
            }
        }
        return reduceaList;
    }

    /**
     * @param newList
     * @param oldList
     * @return Returns a list of added elements
     * @Description:
     * Calculate the addition of the new list NEWLIST relative to the old list oldlist,
     * which is compatible with the list data structure of any type of element
     */
    public static <E> List<E> getAddList(List<E> newList, List<E> oldList) {
        if (oldList == null || oldList.isEmpty()) {
            return newList;
        }
        List<E> addList = new ArrayList<>();
        for (int i = 0; i < newList.size(); i++) {
            if (!listContains(oldList, newList.get(i))) {
                addList.add(newList.get(i));
            }
        }
        return addList;
    }

    /**
     * Determine whether the list contains element elements
     *
     * @param sourceList
     * @param element
     * @param <E>
     * @return
     */
    public static <E> boolean listContains(List<E> sourceList, E element) {
        if (sourceList == null || element == null) {
            return false;
        }
        if (sourceList.isEmpty()) {
            return false;
        }
        for (E tip : sourceList) {
            if (element.equals(tip)) {
                return true;
            }
        }
        return false;
    }
}
