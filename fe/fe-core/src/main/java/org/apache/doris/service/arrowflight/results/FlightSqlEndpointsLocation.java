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

package org.apache.doris.service.arrowflight.results;

import org.apache.doris.analysis.Expr;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import java.util.ArrayList;

public class FlightSqlEndpointsLocation {
    private TUniqueId finstId;
    private TNetworkAddress resultFlightServerAddr;
    private TNetworkAddress resultInternalServiceAddr;
    private TNetworkAddress resultPublicAccessAddr;
    private ArrayList<Expr> resultOutputExprs;

    public FlightSqlEndpointsLocation(TUniqueId finstId, TNetworkAddress resultFlightServerAddr,
            TNetworkAddress resultInternalServiceAddr, ArrayList<Expr> resultOutputExprs) {
        this.finstId = finstId;
        this.resultFlightServerAddr = resultFlightServerAddr;
        this.resultInternalServiceAddr = resultInternalServiceAddr;
        this.resultPublicAccessAddr = new TNetworkAddress();
        this.resultOutputExprs = resultOutputExprs;
    }

    public TUniqueId getFinstId() {
        return finstId;
    }

    public TNetworkAddress getResultFlightServerAddr() {
        return resultFlightServerAddr;
    }

    public TNetworkAddress getResultInternalServiceAddr() {
        return resultInternalServiceAddr;
    }

    public void setResultPublicAccessAddr(TNetworkAddress resultPublicAccessAddr) {
        this.resultPublicAccessAddr = resultPublicAccessAddr;
    }

    public TNetworkAddress getResultPublicAccessAddr() {
        return resultPublicAccessAddr;
    }

    public ArrayList<Expr> getResultOutputExprs() {
        return resultOutputExprs;
    }
}
