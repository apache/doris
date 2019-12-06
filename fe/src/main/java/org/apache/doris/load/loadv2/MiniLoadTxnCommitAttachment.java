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

package org.apache.doris.load.loadv2;

import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TMiniLoadTxnCommitAttachment;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TxnCommitAttachment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MiniLoadTxnCommitAttachment extends TxnCommitAttachment {
    private long loadedRows;
    private long filteredRows;
    // optional
    private String errorLogUrl;

    public MiniLoadTxnCommitAttachment() {
        super(TransactionState.LoadJobSourceType.BACKEND_STREAMING);
    }

    public MiniLoadTxnCommitAttachment(TMiniLoadTxnCommitAttachment tMiniLoadTxnCommitAttachment) {
        super(TransactionState.LoadJobSourceType.BACKEND_STREAMING);
        this.loadedRows = tMiniLoadTxnCommitAttachment.getLoadedRows();
        this.filteredRows = tMiniLoadTxnCommitAttachment.getFilteredRows();
        if (tMiniLoadTxnCommitAttachment.isSetErrorLogUrl()) {
            this.errorLogUrl = tMiniLoadTxnCommitAttachment.getErrorLogUrl();
        }
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public String getErrorLogUrl() {
        return errorLogUrl;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(filteredRows);
        out.writeLong(loadedRows);
        if (errorLogUrl == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, errorLogUrl);
        }

    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        filteredRows = in.readLong();
        loadedRows = in.readLong();
        if (in.readBoolean()) {
            errorLogUrl = Text.readString(in);
        }
    }
}
