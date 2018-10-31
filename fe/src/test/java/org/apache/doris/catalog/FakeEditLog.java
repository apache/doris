package org.apache.doris.catalog;

import org.apache.doris.alter.RollupJob;
import org.apache.doris.alter.SchemaChangeJob;
import org.apache.doris.cluster.Cluster;
import org.apache.doris.persist.EditLog;
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
    
    public TransactionState getTransaction(long transactionId) {
        return allTransactionState.get(transactionId);
    }
}
