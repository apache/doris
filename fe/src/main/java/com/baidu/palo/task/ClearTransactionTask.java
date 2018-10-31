package com.baidu.palo.task;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.thrift.TClearTransactionTaskRequest;
import com.baidu.palo.thrift.TTaskType;

public class ClearTransactionTask extends AgentTask {

    private static final Logger LOG = LogManager.getLogger(ClearTransactionTask.class);

    private long transactionId;
    private List<Long> partitionIds;
    private boolean isFinished;

    public ClearTransactionTask(long backendId, long transactionId, 
            List<Long> partitionIds) {
        super(null, backendId, TTaskType.CLEAR_TRANSACTION_TASK, -1L, -1L, -1L, -1L, -1L, transactionId);
        this.transactionId = transactionId;
        this.partitionIds = partitionIds;
        this.isFinished = false;
    }
    
    public TClearTransactionTaskRequest toThrift() {
        TClearTransactionTaskRequest clearTransactionTaskRequest = new TClearTransactionTaskRequest(transactionId, 
                partitionIds);
        return clearTransactionTaskRequest;
    }
    
    public void setFinished() {
        this.isFinished = true;
    }
    
    public boolean isFinished() {
        return this.isFinished;
    }
}
