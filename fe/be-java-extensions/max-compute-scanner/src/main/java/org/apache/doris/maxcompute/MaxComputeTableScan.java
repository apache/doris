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

package org.apache.doris.maxcompute;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

import java.io.IOException;

/**
 * MaxComputeJ JniScanner. BE will read data from the scanner object.
 */
public class MaxComputeTableScan {
    private static final String odpsUrlTemplate = "http://service.{}.maxcompute.aliyun-inc.com/api";
    private static final String tunnelUrlTemplate = "http://dt.{}.maxcompute.aliyun-inc.com";
    private final Odps odps;
    private final TableTunnel tunnel;
    private final String project;
    private final String table;
    private volatile long readRows = 0;
    private long totalRows = 0;

    public MaxComputeTableScan(String region, String project, String table,
                               String accessKey, String secretKey, boolean enablePublicAccess) {
        this.project = project;
        this.table = table;
        odps = new Odps(new AliyunAccount(accessKey, secretKey));
        String odpsUrl = odpsUrlTemplate.replace("{}", region);
        String tunnelUrl = tunnelUrlTemplate.replace("{}", region);
        if (enablePublicAccess) {
            odpsUrl = odpsUrl.replace("-inc", "");
            tunnelUrl = tunnelUrl.replace("-inc", "");
        }
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(this.project);
        tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(tunnelUrl);
    }

    public TableSchema getSchema() {
        return odps.tables().get(table).getSchema();
    }

    public TableTunnel.DownloadSession openDownLoadSession() throws IOException {
        TableTunnel.DownloadSession tableSession;
        try {
            String downloadId = "DORIS_MC_DOWNLOAD_" + System.currentTimeMillis();
            tableSession = tunnel.getDownloadSession(project, table, downloadId);
            totalRows = tableSession.getRecordCount();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
        return tableSession;
    }

    public TableTunnel.DownloadSession openDownLoadSession(PartitionSpec partitionSpec) throws IOException {
        TableTunnel.DownloadSession tableSession;
        try {
            String downloadId = "DORIS_MC_DOWNLOAD_" + System.currentTimeMillis();
            tableSession = tunnel.getDownloadSession(project, table, partitionSpec, downloadId);
            totalRows = tableSession.getRecordCount();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
        return tableSession;
    }

    public synchronized void increaseReadRows(long rows) {
        // multi-thread writing must be synchronized
        readRows += rows;
    }

    public boolean endOfScan() {
        return readRows >= totalRows;
    }
}
