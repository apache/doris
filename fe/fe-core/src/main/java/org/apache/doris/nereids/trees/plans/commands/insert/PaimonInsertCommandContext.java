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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.HashMap;
import java.util.Map;

/**
 * Insert command context for Paimon tables.
 */
public class PaimonInsertCommandContext extends BaseExternalTableInsertCommandContext {
    private long txnId = 0;
    private String commitUser;
    private Map<String, Expression> staticPartition = new HashMap<>();

    public long getTxnId() {
        return txnId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public String getCommitUser() {
        return commitUser;
    }

    public void setCommitUser(String commitUser) {
        this.commitUser = commitUser;
    }

    public Map<String, Expression> getStaticPartition() {
        return staticPartition;
    }

    public void setStaticPartition(Map<String, Expression> staticPartition) {
        this.staticPartition = staticPartition != null
                ? new HashMap<>(staticPartition) : new HashMap<>();
    }
}
