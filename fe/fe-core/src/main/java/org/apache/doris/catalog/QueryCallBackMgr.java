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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class is suitable for queries, especially for releasing resources after the query is completed.
 * The resources generally need to be held from the plan to the end of the query.
 *
 * For example, when reading a Hive ACID table, you should first acquire a lock from the HMS.
 * Releasing the lock on the HMS is divided into two parts:
 *      1. The plan fails.
 *      2. The plan is sent to the BE (backend), and the BE finishes reading.
 */
public class QueryCallBackMgr {
    //query id => plan fail callback func.
    // In the same query, different plan nodes may have different callback functions, so handle a List<Runnable>.
    private Map<TUniqueId, List<Runnable>> planFailCallBackMap = Maps.newConcurrentMap();
    //query id => query end callback func .(plan success).
    private Map<TUniqueId, List<Runnable>> queryEndCallBackMap = Maps.newConcurrentMap();

    public void registerPlanFailFunc(TUniqueId queryId, Runnable callback) {
        planFailCallBackMap.computeIfAbsent(queryId,
                k -> Collections.synchronizedList(new ArrayList<>())).add(callback);
    }

    public void registerQueryEndFunc(TUniqueId queryId, Runnable callback) {
        queryEndCallBackMap.computeIfAbsent(queryId,
                k -> Collections.synchronizedList(new ArrayList<>())).add(callback);
    }

    public void planFailCallback(TUniqueId queryId) {
        List<Runnable> callbacks = planFailCallBackMap.remove(queryId);
        if (callbacks != null) {
            for (Runnable callback : callbacks) {
                callback.run();
            }
        }
        queryEndCallBackMap.remove(queryId);
    }

    public void queryEndCallback(TUniqueId queryId) {
        List<Runnable> callbacks = queryEndCallBackMap.remove(queryId);
        if (callbacks != null) {
            for (Runnable callback : callbacks) {
                callback.run();
            }
        }
        planFailCallBackMap.remove(queryId);
    }
}
