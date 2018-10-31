package com.baidu.palo.task;

import com.baidu.palo.thrift.TClearAlterTaskRequest;
import com.baidu.palo.thrift.TTaskType;

public class ClearAlterTask extends AgentTask {
    private int schemaHash;
    private boolean isFinished;

    public ClearAlterTask(long backendId, long dbId, long tableId, long partitionId, long indexId,
                            long tabletId, int schemaHash) {
        super(null, backendId, TTaskType.CLEAR_ALTER_TASK, dbId, tableId, partitionId, indexId, tabletId);

        this.schemaHash = schemaHash;
        this.isFinished = false;
    }

    public TClearAlterTaskRequest toThrift() {
        TClearAlterTaskRequest request = new TClearAlterTaskRequest(tabletId, schemaHash);
        return request;
    }

    public int getSchemaHash() {
        return schemaHash;
    }
    
    public void setFinished() {
        this.isFinished = true;
    }
    
    public boolean isFinished() {
        return isFinished;
    }
}
