package org.apache.doris.job.extensions.cdc;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.planner.DataGenScanNode;
import org.apache.doris.planner.ScanNode;

import java.util.List;

@Slf4j
public class CdcTableTask extends InsertTask {

    public CdcTableTask(String labelName, String currentDbName, String executeSql, UserIdentity createUser) {
        super(labelName, currentDbName, executeSql, createUser);
    }

    @Override
    public void run() throws JobException {
        try{
            AbstractInsertExecutor insertExecutor = command.initPlan(ctx, stmtExecutor);
            List<ScanNode> scanNodes = stmtExecutor.planner().getScanNodes();
            for(ScanNode scanNode : scanNodes){
                //todo: Set the hose and port of cdc scannode
                if(scanNode instanceof DataGenScanNode){
                    DataGenScanNode dataGenScanNode = (DataGenScanNode) scanNode;
                    //set host and ip
                }
            }
            insertExecutor.executeSingleInsert(stmtExecutor, getTaskId());
        }catch (Exception ex){
            log.warn("execute insert task error, job id is {}, task id is {},sql is {}", getJobId(),
                getTaskId(), sql, ex);
            throw new JobException(ex);
        }
    }

    @Override
    public void onSuccess() throws JobException {
        super.onSuccess();
        System.out.println("task success");
    }

    @Override
    public void onFail() throws JobException {
        super.onFail();
        System.out.println("task fail");
    }
}
