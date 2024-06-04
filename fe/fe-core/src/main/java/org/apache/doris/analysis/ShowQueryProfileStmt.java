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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

// deprecated stmt, use will be guided to a specific url to get profile from
// web browser
public class ShowQueryProfileStmt extends ShowStmt {
    private String queryIdPath;

    public ShowQueryProfileStmt(String queryIdPath) {
        this.queryIdPath = queryIdPath;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        String selfHost = Env.getCurrentEnv().getSelfNode().getHost();
        int httpPort = Config.http_port;
        String terminalMsg = String.format(
                "try visit http://%s:%d/QueryProfile/%s, show query/load profile syntax is a deprecated feature",
                selfHost, httpPort, this.queryIdPath);
        throw new UserException(terminalMsg);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return null;
    }
}
