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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.loadv2.LoadJobFinalOperation;
import org.apache.doris.load.loadv2.MiniLoadTxnCommitAttachment;
import org.apache.doris.load.routineload.RLTaskTxnCommitAttachment;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TTxnCommitAttachment;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class TxnCommitAttachment implements Writable, GsonPostProcessable {
    @SerializedName(value = "sourceType")
    protected TransactionState.LoadJobSourceType sourceType;
    protected boolean isTypeRead = false;

    public TxnCommitAttachment(TransactionState.LoadJobSourceType sourceType) {
        this.sourceType = sourceType;
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public static TxnCommitAttachment fromThrift(TTxnCommitAttachment txnCommitAttachment) {
        if (txnCommitAttachment != null) {
            switch (txnCommitAttachment.getLoadType()) {
                case ROUTINE_LOAD:
                    return new RLTaskTxnCommitAttachment(txnCommitAttachment.getRlTaskTxnCommitAttachment());
                default:
                    return null;
            }
        } else {
            return null;
        }
    }

    public static TxnCommitAttachment read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_137) {
            TxnCommitAttachment attachment = null;
            LoadJobSourceType type = LoadJobSourceType.valueOf(Text.readString(in));
            if (type == LoadJobSourceType.ROUTINE_LOAD_TASK) {
                attachment = new RLTaskTxnCommitAttachment();
            } else if (type == LoadJobSourceType.BATCH_LOAD_JOB) {
                attachment = new LoadJobFinalOperation();
            } else if (type == LoadJobSourceType.BACKEND_STREAMING) {
                attachment = new MiniLoadTxnCommitAttachment();
            } else if (type == LoadJobSourceType.FRONTEND) {
                // spark load
                attachment = new LoadJobFinalOperation();
            } else {
                throw new IOException("Unknown load job source type: " + type.name());
            }

            attachment.setTypeRead(true);
            attachment.readFields(in);
            return attachment;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), TxnCommitAttachment.class);
        }
    }

    public void gsonPostProcess() {
        setTypeRead(true);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            sourceType = LoadJobSourceType.valueOf(Text.readString(in));
            isTypeRead = true;
        }
    }
}
