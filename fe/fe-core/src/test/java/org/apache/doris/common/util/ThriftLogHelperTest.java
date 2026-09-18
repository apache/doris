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

import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TStreamLoadPutRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ThriftLogHelperTest {
    private static final String MASKED_CREDENTIAL = "***MASKED***";

    @Test
    public void testRequestForLogMasksCredentialsByFieldName() {
        TLoadTxnBeginRequest beginRequest = new TLoadTxnBeginRequest()
                .setUser("user")
                .setPasswd("password")
                .setDb("db")
                .setTbl("table")
                .setLabel("label")
                .setToken("token")
                .setAuthCode(123)
                .setAuthCodeUuid("auth-code-uuid");

        TLoadTxnBeginRequest beginRequestForLog = ThriftLogHelper.requestForLog(beginRequest);

        Assertions.assertEquals(MASKED_CREDENTIAL, beginRequestForLog.getPasswd());
        Assertions.assertEquals(MASKED_CREDENTIAL, beginRequestForLog.getToken());
        Assertions.assertEquals(MASKED_CREDENTIAL, beginRequestForLog.getAuthCodeUuid());
        Assertions.assertFalse(beginRequestForLog.isSetAuthCode());
        Assertions.assertEquals("user", beginRequestForLog.getUser());
        Assertions.assertEquals("label", beginRequestForLog.getLabel());
        Assertions.assertEquals("password", beginRequest.getPasswd());
        Assertions.assertEquals("token", beginRequest.getToken());
        Assertions.assertEquals(123, beginRequest.getAuthCode());
        Assertions.assertEquals("auth-code-uuid", beginRequest.getAuthCodeUuid());

        TStreamLoadPutRequest putRequest = new TStreamLoadPutRequest()
                .setUser("load-user")
                .setPasswd("load-password")
                .setDb("load-db")
                .setTbl("load-table")
                .setToken("load-token")
                .setAuthCode(456);

        TStreamLoadPutRequest putRequestForLog = ThriftLogHelper.requestForLog(putRequest);

        Assertions.assertEquals(MASKED_CREDENTIAL, putRequestForLog.getPasswd());
        Assertions.assertEquals(MASKED_CREDENTIAL, putRequestForLog.getToken());
        Assertions.assertFalse(putRequestForLog.isSetAuthCode());
        Assertions.assertEquals("load-user", putRequestForLog.getUser());
        Assertions.assertEquals("load-table", putRequestForLog.getTbl());
        Assertions.assertEquals("load-password", putRequest.getPasswd());
        Assertions.assertEquals("load-token", putRequest.getToken());
        Assertions.assertEquals(456, putRequest.getAuthCode());
    }
}
