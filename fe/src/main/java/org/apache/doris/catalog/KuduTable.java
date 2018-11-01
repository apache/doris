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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.KuduUtil;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class KuduTable extends Table {
    private static final Logger LOG = LogManager.getLogger(KuduTable.class);

    private org.apache.kudu.client.KuduTable table;

    private KuduPartition rangePartition;
    private KuduPartition hashPartition;

    private String kuduTableId;
    private String masterAddrs;
    private byte[] tableLock;
    private boolean isObsolate;

    public KuduTable(long id, List<Column> baseSchema, org.apache.kudu.client.KuduTable table) {
        super(id, table.getName(), TableType.KUDU, baseSchema);
        this.table = table;
        tableLock = new byte[0];
        isObsolate = false;
    }

    public KuduTable() {
        super(TableType.KUDU);
        tableLock = new byte[0];
    }

    public void setRangeParititon(KuduPartition rangePartition) {
        this.rangePartition = rangePartition;
    }

    public void setHashPartition(KuduPartition hashPartition) {
        this.hashPartition = hashPartition;
    }

    public void setMasterAddrs(String masterAddrs) {
        this.masterAddrs = masterAddrs;
    }

    /*
     * if table is null(this may happend when fe restart), try open table from kudu master,
     * if failed to open table, mark this kudu table as Obsolate
     */
    public org.apache.kudu.client.KuduTable getKuduTable() {
        if (isObsolate) {
            return null;
        }

        if (table == null) {
            synchronized (tableLock) {
                if (table == null) {
                    KuduClient client = KuduUtil.createKuduClient(masterAddrs);
                    try {
                        table = client.openTable(name);
                    } catch (KuduException e) {
                        LOG.warn("Failed to open kudu table: {}", name, e);
                        isObsolate = true;
                        return null;
                    }

                    if (!table.getTableId().equalsIgnoreCase(kuduTableId)) {
                        LOG.warn("kudu table has changed: {}", name);
                        isObsolate = true;
                        return null;
                    }
                }
            }
        }
        return table;
    }

    public void setKuduTableId(String tableId) {
        this.kuduTableId = tableId;
    }

    public KuduPartition getRangePartition() {
        return rangePartition;
    }

    public KuduPartition getHashPartition() {
        return hashPartition;
    }

    public String getKuduTableId() {
        return kuduTableId;
    }

    public String getMasterAddrs() {
        return masterAddrs;
    }

    public boolean isObsolate() {
        return isObsolate;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, kuduTableId);
        Text.writeString(out, masterAddrs);
        out.writeBoolean(isObsolate);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        kuduTableId = Text.readString(in);
        masterAddrs = Text.readString(in);
        isObsolate = in.readBoolean();
    }
}
