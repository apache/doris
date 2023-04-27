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

package org.apache.doris.qe;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// Queue of QueryDetail.
// It's used to collect queries for monitor.
// The default copacity is 10000.
public class QueryDetailQueue {
    private static Map<String, QueryDetail> runningQueries = Maps.newHashMap();
    private static LinkedList<QueryDetail> totalQueries = new LinkedList<QueryDetail>();
    private static int queryCapacity = 10000;

    public static synchronized void addOrUpdateQueryDetail(QueryDetail queryDetail) {
        if (runningQueries.get(queryDetail.getQueryId()) == null) {
            if (queryDetail.getState() == QueryDetail.QueryMemState.RUNNING) {
                runningQueries.put(queryDetail.getQueryId(), queryDetail);
                totalQueries.add(queryDetail);
            } else {
                totalQueries.add(queryDetail);
            }
        } else {
            if (queryDetail.getState() != QueryDetail.QueryMemState.RUNNING) {
                QueryDetail qDetail = runningQueries.remove(queryDetail.getQueryId());
                qDetail.setLatency(queryDetail.getLatency());
                qDetail.setState(queryDetail.getState());
            }
        }
        if (totalQueries.size() > queryCapacity) {
            QueryDetail qDetail = totalQueries.remove();
            runningQueries.remove(qDetail.getQueryId());
        }
    }

    public static synchronized List<QueryDetail> getQueryDetails(long eventTime) {
        List<QueryDetail> results = Lists.newArrayList();
        Iterator<QueryDetail> it = totalQueries.iterator();
        while (it.hasNext()) {
            QueryDetail queryDetail = it.next();
            if (queryDetail.getEventTime() > eventTime) {
                results.add(queryDetail);
            }
        }
        return results;
    }

}
