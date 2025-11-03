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

import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class QueryDetailQueueTest {
    @Test
    public void testQueryDetailQueue() {
        long eventTime = 1592208814796L;
        QueryDetail queryDetail = new QueryDetail(eventTime, "219a2d5443c542d4-8fc938db37c892e3",
                                                  eventTime, -1, -1, QueryDetail.QueryMemState.RUNNING,
                                                  "testDb", "select * from table1 limit 1");
        QueryDetailQueue.addOrUpdateQueryDetail(queryDetail);

        List<QueryDetail> queryDetails = QueryDetailQueue.getQueryDetails(eventTime);
        Assert.assertTrue(queryDetails.size() == 0);

        queryDetails = QueryDetailQueue.getQueryDetails(eventTime - 1);
        Assert.assertTrue(queryDetails.size() == 1);

        Gson gson = new Gson();
        String jsonString = gson.toJson(queryDetails);
        String queryDetailString = "[{\"eventTime\":1592208814796,"
                                     + "\"queryId\":\"219a2d5443c542d4-8fc938db37c892e3\","
                                     + "\"startTime\":1592208814796,\"endTime\":-1,\"latency\":-1,"
                                     + "\"state\":\"RUNNING\",\"database\":\"testDb\","
                                     + "\"sql\":\"select * from table1 limit 1\"}]";
        Assert.assertEquals(jsonString, queryDetailString);

        queryDetail.setEventTime(eventTime + 1);
        queryDetail.setEndTime(eventTime + 1);
        queryDetail.setLatency(1);
        queryDetail.setState(QueryDetail.QueryMemState.FINISHED);
        QueryDetailQueue.addOrUpdateQueryDetail(queryDetail);

        queryDetails = QueryDetailQueue.getQueryDetails(eventTime);
        Assert.assertTrue(queryDetails.size() == 1);

        jsonString = gson.toJson(queryDetails);
        queryDetailString = "[{\"eventTime\":1592208814797,"
                              + "\"queryId\":\"219a2d5443c542d4-8fc938db37c892e3\","
                              + "\"startTime\":1592208814796,\"endTime\":1592208814797,"
                              + "\"latency\":1,\"state\":\"FINISHED\",\"database\":\"testDb\","
                              + "\"sql\":\"select * from table1 limit 1\"}]";
        Assert.assertEquals(jsonString, queryDetailString);
    }
}
