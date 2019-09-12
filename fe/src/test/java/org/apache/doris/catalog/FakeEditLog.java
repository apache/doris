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

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.alter.RollupJob;
import org.apache.doris.alter.SchemaChangeJob;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.RoutineLoadOperation;
import org.apache.doris.transaction.TransactionState;

import java.util.HashMap;
import java.util.Map;

import mockit.Mock;
import mockit.MockUp;

public class FakeEditLog extends MockUp<EditLog> {
    
    private Map<Long, TransactionState> allTransactionState = new HashMap<>();
    
    @Mock
    public void $init(String nodeName) {
        // do nothing
        System.out.println("abc");
    }
    
    @Mock
    public void logInsertTransactionState(TransactionState transactionState) {
        // do nothing
        System.out.println("insert transaction manager is called");
        allTransactionState.put(transactionState.getTransactionId(), transactionState);
    }

    @Mock
    public void logDeleteTransactionState(TransactionState transactionState) {
        // do nothing
        System.out.println("delete transaction state is deleted");
        allTransactionState.remove(transactionState.getTransactionId());
    }
    
    @Mock
    public void logSaveNextId(long nextId) {
        // do nothing
    }
    
    @Mock
    public void logCreateCluster(Cluster cluster) {
        // do nothing
    }
    
    @Mock
    public void logStartRollup(RollupJob rollupJob) {
        
    }

    @Mock
    public void logFinishingRollup(RollupJob rollupJob) {
        
    }
    
    @Mock
    public void logCancelRollup(RollupJob rollupJob) {
    }
    
    @Mock
    public void logStartSchemaChange(SchemaChangeJob schemaChangeJob) {
    }
    
    @Mock
    public void logFinishingSchemaChange(SchemaChangeJob schemaChangeJob) {
    }
    
    @Mock
    public void logOpRoutineLoadJob(RoutineLoadOperation operation) {

    }

    @Mock
    public void logAlterJob(AlterJobV2 alterJob) {

    }

    public TransactionState getTransaction(long transactionId) {
        return allTransactionState.get(transactionId);
    }
}
