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

package org.apache.doris.service.arrowflight;

import org.apache.doris.service.arrowflight.sessions.FlightSessionsManager;

import org.apache.arrow.flight.FlightProducer.CallContext;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class FlightSqlParameterBindingTest {
    @Test
    public void acceptsParameterUpload() throws Exception {
        try (DorisFlightSqlProducer producer = new DorisFlightSqlProducer(
                Location.forGrpcInsecure("127.0.0.1", 0), Mockito.mock(FlightSessionsManager.class))) {
            Assert.assertNotNull(producer.acceptPutPreparedStatementQuery(
                    CommandPreparedStatementQuery.getDefaultInstance(), Mockito.mock(CallContext.class),
                    Mockito.mock(FlightStream.class), Mockito.mock(StreamListener.class)));
        }
    }
}
