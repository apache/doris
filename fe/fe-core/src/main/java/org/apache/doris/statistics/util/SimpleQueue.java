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

package org.apache.doris.statistics.util;

import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Function;

// Any operation on this structure should be thread-safe
public class SimpleQueue<T> extends LinkedList<T> {

    private final long limit;

    private final Function<T, Void> offerFunc;

    private final Function<T, Void> evictFunc;


    public SimpleQueue(long limit, Function<T, Void> offerFunc, Function<T, Void> evictFunc) {
        this.limit = limit;
        this.offerFunc = offerFunc;
        this.evictFunc = evictFunc;
    }

    @Override
    public synchronized boolean offer(T analysisInfo) {
        while (size() >= limit) {
            remove();
        }
        super.offer(analysisInfo);
        offerFunc.apply(analysisInfo);
        return true;
    }

    @Override
    public synchronized T remove() {
        T analysisInfo = super.remove();
        evictFunc.apply(analysisInfo);
        return analysisInfo;
    }

    public SimpleQueue(long limit, Function<T, Void> offerFunc, Function<T, Void> evictFunc, Collection<T> collection) {
        this(limit, offerFunc, evictFunc);
        if (collection != null) {
            for (T e : collection) {
                offer(e);
            }
        }
    }
}
