package org.apache.doris.scheduler;

import org.apache.doris.analysis.CreateJobStmt;
import org.apache.doris.scheduler.job.Job;

public class SqlJobClient {
    
    public void createJob(CreateJobStmt stmt){
        Job job = stmt.getJob();
        AsyncJobRegister register= (AsyncJobRegister) JobRegisterFactory.getInstance();
        if(job.isCycleJob()){
            
        }
        
    }
}
