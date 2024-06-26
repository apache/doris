package org.apache.doris.job.extensions.cdc.utils;

import org.apache.doris.catalog.Env;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CdcService {
    private static final Logger LOG = LogManager.getLogger(CdcService.class);
    public static final long HEARTBEAT_TIME = 10000L;
    public static final int HEARTBEAT_MAX_RETRIES = 3;
    public static final long START_CDC_SERVER_TIMEOUT = 60000L;

    public static Backend selectBackend() throws JobException {
        Backend backend = null;
        BeSelectionPolicy policy = null;
        Set<Tag> userTags = new HashSet<>();
        if(ConnectContext.get() != null){
            String qualifiedUser = ConnectContext.get().getQualifiedUser();
            userTags  = Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser);
        }

        policy = new BeSelectionPolicy.Builder()
            .addTags(userTags)
            .setEnableRoundRobin(true)
            .needLoadAvailable().build();
        List<Long> backendIds;
        backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (backendIds.isEmpty()) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        backend = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return backend;
    }
}
