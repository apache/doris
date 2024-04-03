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

package org.apache.doris.common.proc;

import org.apache.doris.binlog.BinlogManager;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;


public class BinlogProcDir implements ProcDirInterface {
    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        throw new AnalysisException("not implemented");
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BinlogManager binlogManager = Env.getCurrentEnv().getBinlogManager();
        if (binlogManager == null) {
            throw new AnalysisException("binlog manager is not initialized");
        }

        return binlogManager.getBinlogInfo();
    }
}
