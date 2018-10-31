package com.baidu.palo.task;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.thrift.TPartitionVersionInfo;
import com.baidu.palo.thrift.TPublishVersionRequest;
import com.baidu.palo.thrift.TTaskType;

public class PublishVersionTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PublishVersionTask.class);

    private long transactionId;
    private List<TPartitionVersionInfo> partitionVersionInfos;
    private List<Long> errorTablets;
    private boolean isFinished;

    public PublishVersionTask(long backendId, long transactionId, 
            List<TPartitionVersionInfo> partitionVersionInfos) {
        super(null, backendId, TTaskType.PUBLISH_VERSION, -1L, -1L, -1L, -1L, -1L, transactionId);
        this.transactionId = transactionId;
        this.partitionVersionInfos = partitionVersionInfos;
        this.errorTablets = new ArrayList<Long>();
        this.isFinished = false;
    }
    
    public TPublishVersionRequest toThrift() {
        TPublishVersionRequest publishVersionRequest = new TPublishVersionRequest(transactionId, 
                partitionVersionInfos);
        return publishVersionRequest;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public List<TPartitionVersionInfo> getPartitionVersionInfos() {
        return partitionVersionInfos;
    }

    public List<Long> getErrorTablets() {
        return errorTablets;
    }
    
    public void addErrorTablets(List<Long> errorTablets) {
        if (errorTablets == null) {
            return;
        }
        this.errorTablets.addAll(errorTablets);
    }
    
    public void setIsFinished(boolean isFinished) {
        this.isFinished = isFinished;
    }
    
    public boolean isFinished() {
        return isFinished;
    }
}
