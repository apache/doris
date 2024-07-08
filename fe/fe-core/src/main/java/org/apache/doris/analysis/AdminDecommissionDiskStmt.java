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

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * ADMIN DECOMMISSION DISK '/disk/root/path' ON 'backend-id';
 * ADMIN DECOMMISSION DISK FORCE '/disk/root/path' ON 'backend-id';
 */
public class AdminDecommissionDiskStmt extends DdlStmt {
    private final long backendId;
    private final List<String> diskList;
    private final boolean isForce;
    private Backend backend;

    public AdminDecommissionDiskStmt(String backend, List<String> diskList, boolean isForce) {
        this.backendId = Long.parseLong(backend);
        this.diskList = diskList;
        this.isForce = isForce;
    }

    public Backend getBackend() {
        Preconditions.checkState(isAnalyzed());
        return backend;
    }

    public List<String> getDiskList() {
        return diskList;
    }

    public boolean isForce() {
        return isForce;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        backend = Env.getCurrentSystemInfo().getBackend(backendId);
        if (backend == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_VALUE, "Unrecognized Backend");
        }

        int onlineDiskCount = 0;
        for (String disk : diskList) {
            DiskInfo diskInfo = backend.getDisks().get(disk);
            if (diskInfo == null) {
                ErrorReport.reportAnalysisException("Specified disk path (%s) is not configured for BE %s (%s)",
                        ErrorCode.ERR_INVALID_VALUE, disk, backendId, backend.getHost());
            }

            if (diskInfo.getState() != DiskInfo.DiskState.ONLINE && diskInfo.getState() != DiskInfo.DiskState.OFFLINE) {
                ErrorReport.reportAnalysisException("Specified disk path (%s) is not eligible for decommissioning,"
                        + " current state is (%s)",
                        ErrorCode.ERR_INVALID_VALUE, disk, diskInfo.getState().name());
            }

            if (diskInfo.getState() == DiskInfo.DiskState.ONLINE) {
                onlineDiskCount++;
            }
        }

        if (onlineDiskCount == diskList.size() && !isForce) {
            ErrorReport.reportAnalysisException("Specified disk path/s are only remaining alive disk and cannot be"
                    + " removed, use FORCE or have spare disks while migrating", ErrorCode.ERR_INVALID_VALUE);
        }
        super.analyze(analyzer);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
