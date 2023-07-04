package org.apache.doris.scheduler.executor;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.thrift.TUniqueId;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SqlJobExecutor implements JobExecutor {


    private ConnectContext ctx;

    @Getter
    @Setter
    @SerializedName(value = "sql")
    private String sql;

    @Override
    public Object execute(Job job) {
        ctx=new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(ClusterNamespace.getClusterNameFromFullName(job.getDbName()));
        ctx.setDatabase(job.getDbName());
        ctx.setQualifiedUser(job.getUser());
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(job.getUser(), "%"));
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        ctx.getState().reset();
        String taskIdString = UUID.randomUUID().toString();
        UUID taskId = UUID.fromString(taskIdString);
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        ctx.setQueryId(queryId);
        try {
            StmtExecutor executor = new StmtExecutor(ctx, sql);
            executor.execute(queryId);

            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
