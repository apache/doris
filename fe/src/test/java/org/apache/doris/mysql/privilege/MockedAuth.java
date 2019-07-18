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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import mockit.NonStrictExpectations;

public class MockedAuth {

    public static void mockedAuth(PaloAuth auth) {
        new NonStrictExpectations() {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                result = true;

                // auth.checkHasPriv((ConnectContext) any,, priv, levels)
            }
        };
    }

    public static void mockedConnectContext(ConnectContext ctx, String user, String ip) {
        new NonStrictExpectations() {
            {
                ConnectContext.get();
                result = ctx;

                ctx.getQualifiedUser();
                result = user;

                ctx.getRemoteIP();
                result = ip;

                ctx.getState();
                result = new QueryState();

                ctx.getCurrentUserIdentity();
                UserIdentity userIdentity = new UserIdentity(user, ip);
                userIdentity.setIsAnalyzed();
                result = userIdentity;
            }
        };
    }
}
