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

import org.apache.doris.thrift.TMasterOpRequest;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests that connectAttributes (e.g. scheduleInfo for Dataworks lineage)
 * survive the FE-to-FE forward path via TMasterOpRequest.
 */
public class ConnectAttributesForwardTest {

    @Test
    public void testSerializeConnectAttributesToRequest() {
        ConnectContext ctx = new ConnectContext();
        Map<String, String> attrs = new HashMap<>();
        attrs.put("scheduleInfo",
                "{\"SKYNET_TASKID\":\"523987416281\",\"SKYNET_APP_ID\":\"392426\"}");
        attrs.put("_client_name", "dataworks-connector");
        ctx.setConnectAttributes(attrs);

        TMasterOpRequest params = new TMasterOpRequest();
        Map<String, String> connectAttributes = ctx.getConnectAttributes();
        if (connectAttributes != null && !connectAttributes.isEmpty()) {
            params.setConnectAttributes(connectAttributes);
        }

        Assert.assertTrue("connect_attributes should be set on request",
                params.isSetConnectAttributes());
        Assert.assertEquals(2, params.getConnectAttributes().size());
        Assert.assertEquals(
                "{\"SKYNET_TASKID\":\"523987416281\",\"SKYNET_APP_ID\":\"392426\"}",
                params.getConnectAttributes().get("scheduleInfo"));
    }

    @Test
    public void testRestoreConnectAttributesFromRequest() {
        TMasterOpRequest request = new TMasterOpRequest();
        Map<String, String> attrs = new HashMap<>();
        attrs.put("scheduleInfo", "{\"SKYNET_TASKID\":\"523987416281\"}");
        request.setConnectAttributes(attrs);

        ConnectContext ctx = new ConnectContext();
        Assert.assertTrue(ctx.getConnectAttributes().isEmpty());

        if (request.isSetConnectAttributes()) {
            ctx.setConnectAttributes(request.getConnectAttributes());
        }

        Assert.assertEquals(1, ctx.getConnectAttributes().size());
        Assert.assertEquals("{\"SKYNET_TASKID\":\"523987416281\"}",
                ctx.getConnectAttributes().get("scheduleInfo"));
    }

    @Test
    public void testScheduleInfoRoundTrip() {
        String scheduleInfoValue = "{\"SKYNET_ONDUTY\":\"1107550004253538\","
                + "\"ALISA_TASK_ID\":\"T3_0068646895\","
                + "\"SKYNET_TASKID\":\"523987416281\"}";

        ConnectContext senderCtx = new ConnectContext();
        Map<String, String> clientAttrs = new HashMap<>();
        clientAttrs.put("scheduleInfo", scheduleInfoValue);
        senderCtx.setConnectAttributes(clientAttrs);

        TMasterOpRequest request = new TMasterOpRequest();
        Map<String, String> connectAttributes = senderCtx.getConnectAttributes();
        if (connectAttributes != null && !connectAttributes.isEmpty()) {
            request.setConnectAttributes(connectAttributes);
        }

        ConnectContext receiverCtx = new ConnectContext();
        if (request.isSetConnectAttributes()) {
            receiverCtx.setConnectAttributes(request.getConnectAttributes());
        }

        Assert.assertEquals(scheduleInfoValue,
                receiverCtx.getConnectAttributes().get("scheduleInfo"));
    }

    @Test
    public void testEmptyConnectAttributesNotSerialized() {
        ConnectContext ctx = new ConnectContext();

        TMasterOpRequest params = new TMasterOpRequest();
        Map<String, String> connectAttributes = ctx.getConnectAttributes();
        if (connectAttributes != null && !connectAttributes.isEmpty()) {
            params.setConnectAttributes(connectAttributes);
        }

        Assert.assertFalse("connect_attributes should NOT be set for empty attributes",
                params.isSetConnectAttributes());
    }

    @Test
    public void testNoConnectAttributesInRequest() {
        TMasterOpRequest request = new TMasterOpRequest();

        ConnectContext ctx = new ConnectContext();
        if (request.isSetConnectAttributes()) {
            ctx.setConnectAttributes(request.getConnectAttributes());
        }

        Assert.assertNotNull(ctx.getConnectAttributes());
        Assert.assertTrue(ctx.getConnectAttributes().isEmpty());
    }
}
