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

package org.apache.doris.regression.util

import com.sun.org.apache.xpath.internal.operations.Bool
import groovy.lang.Tuple2
import groovyjarjarantlr4.v4.runtime.misc.Tuple2
import org.apache.doris.regression.suite.FrontendClientImpl
import org.apache.doris.thrift.TGetBinlogRequest
import org.apache.doris.thrift.TStatusCode
import org.apache.doris.thrift.TBinlog
import org.apache.doris.thrift.TGetBinlogResult
import org.apache.doris.thrift.TStatus

class SyncerUtils {
    static TGetBinlogResult getBinLog(FrontendClientImpl clientImpl) {
        TGetBinlogRequest request = new TGetBinlogRequest(clientImpl.user, clientImpl.passwd, clientImpl.db, clientImpl.seq)
        clientImpl.client.getBinlog(request)
    }
}
