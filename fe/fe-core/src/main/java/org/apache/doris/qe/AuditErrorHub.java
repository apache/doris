package org.apache.doris.qe;

import org.apache.doris.catalog.Env;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;

// This class is used to record recent errors related to audit log.
// And use can query the error log by system table:
// select * from information_schema.audit_error_hub;
public class AuditErrorHub {

    // A thread-safe queue to store the error info
    // The queue size is up to 100, and if exceeded, the oldest error will be removed.
    // The EvictingQueue is not thread safe, so we need to synchronize the access to it.
    private  final EvictingQueue<AuditError> errorsQueue = EvictingQueue.create(100);

    public AuditErrorHub() {

    }

    public synchronized void addError(String message) {
        errorsQueue.add(new AuditError(message));
    }

    public synchronized List<AuditError> getCopiedErrors() {
        return Lists.newArrayList(errorsQueue);
    }

    @Getter
    public static class AuditError {
        private final String feNode; // FE IP:editLogPort
        private final long timestamp;
        private final String message;

        public AuditError(String message) {
            this.feNode = Env.getCurrentEnv().getSelfNode().toIpPort();
            this.timestamp = System.currentTimeMillis();
            this.message = message;
        }
    }
}


