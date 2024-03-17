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

package org.apache.doris.cloud.load;

import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.LoadJobScheduler;
import org.apache.doris.load.loadv2.LoadManager;

public class CloudLoadManager extends LoadManager {

    public CloudLoadManager(LoadJobScheduler loadJobScheduler) {
        super(loadJobScheduler);
    }

    @Override
    public long createLoadJobFromStmt(LoadStmt stmt) throws DdlException, UserException {
        CloudSystemInfoService.waitForAutoStartCurrentCluster();

        return super.createLoadJobFromStmt(stmt);
    }

    @Override
    public long createLoadJobFromStmt(InsertStmt stmt) throws DdlException {
        CloudSystemInfoService.waitForAutoStartCurrentCluster();

        return super.createLoadJobFromStmt(stmt);
    }

}

