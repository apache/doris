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

package org.apache.doris.planner;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TUniqueId;

import com.google.protobuf.ByteString;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;

public class GroupCommitPlannerTest {

    @Test
    public void testSerializeWithLatestStatementQueryGlobals() throws TException {
        TPipelineFragmentParamsList paramsList = new TPipelineFragmentParamsList();
        TPipelineFragmentParams params = new TPipelineFragmentParams();
        params.setProtocolVersion(PaloInternalServiceVersion.V1);
        params.setQueryId(new TUniqueId(1, 2));
        params.setPerExchNumSenders(new HashMap<>());
        params.setDestinations(new ArrayList<>());
        params.setLocalParams(new ArrayList<>());
        params.setQueryGlobals(new TQueryGlobals());
        paramsList.addToParamsList(params);

        assertSerializedQueryGlobals(paramsList,
                new StatementContext(null, null, Instant.ofEpochSecond(100, 123456000),
                        ZoneId.of("Asia/Shanghai")),
                100123, 123456000, "Asia/Shanghai");
        assertSerializedQueryGlobals(paramsList,
                new StatementContext(null, null, Instant.ofEpochSecond(200, 654321000),
                        ZoneId.of("UTC")),
                200654, 654321000, "UTC");
    }

    private void assertSerializedQueryGlobals(TPipelineFragmentParamsList paramsList,
            StatementContext statementContext, long expectedTimestampMs, int expectedNanoSeconds,
            String expectedTimeZone) throws TException {
        ByteString serialized = GroupCommitPlanner.serializeWithQueryGlobals(paramsList, statementContext);
        TPipelineFragmentParamsList deserialized = new TPipelineFragmentParamsList();
        new TDeserializer().deserialize(deserialized, serialized.toByteArray());

        TQueryGlobals queryGlobals = deserialized.getParamsList().get(0).getQueryGlobals();
        Assert.assertEquals(expectedTimestampMs, queryGlobals.getTimestampMs());
        Assert.assertEquals(expectedNanoSeconds, queryGlobals.getNanoSeconds());
        Assert.assertEquals(expectedTimeZone, queryGlobals.getTimeZone());
    }
}
