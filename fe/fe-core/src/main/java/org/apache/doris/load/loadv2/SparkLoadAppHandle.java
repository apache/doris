package org.apache.doris.load.loadv2;

import com.google.common.collect.Lists;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;

public class SparkLoadAppHandle {
    private static final Logger LOG = LogManager.getLogger(SparkLoadAppHandle.class);
    // 5min
    private static final long SUBMIT_APP_TIMEOUT_MS = 300 * 1000;

    private Process process;

    private String appId;
    private State state;
    private String queue;
    private long startTime;
    private FinalApplicationStatus finalStatus;
    private String trackingUrl;
    private String user;

    private List<Listener> listeners;

    public interface Listener {
        void stateChanged(SparkLoadAppHandle handle);

        void infoChanged(SparkLoadAppHandle handle);
    }

    public static enum State {
        UNKNOWN(false),
        CONNECTED(false),
        SUBMITTED(false),
        RUNNING(false),
        FINISHED(true),
        FAILED(true),
        KILLED(true),
        LOST(true);

        private final boolean isFinal;

        private State(boolean isFinal) {
            this.isFinal = isFinal;
        }

        public boolean isFinal() {
            return this.isFinal;
        }
    }

    public SparkLoadAppHandle(Process process) {
        this.state = State.UNKNOWN;
        this.process = process;
    }

    public void addListener(Listener listener) {
        if (this.listeners == null) {
            this.listeners = Lists.newArrayList();
        }

        this.listeners.add(listener);
    }

    public void stop() {
    }

    public void kill() {
        this.setState(State.KILLED);
        if (this.process != null) {
            if (this.process.isAlive()) {
                this.process.destroyForcibly();
            }
            this.process = null;
        }
    }

    public State getState() { return this.state; }

    public String getAppId() { return this.appId; }

    public String getQueue() { return this.queue; }

    public Process getProcess() { return this.process; }

    public long getStartTime() { return this.startTime; }

    public FinalApplicationStatus getFinalStatus() { return this.finalStatus; }

    public String getUrl() { return this.trackingUrl; }

    public String getUser() { return this.user; }

    public void setState(State state) {
        this.state = state;
        this.fireEvent(false);
    }

    public void setAppId(String appId) {
        this.appId = appId;
        this.fireEvent(true);
    }

    public void setQueue(String queue) {
        this.queue = queue;
        this.fireEvent(true);
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
        this.fireEvent(true);
    }

    public void setFinalStatus(FinalApplicationStatus status) {
        this.finalStatus = status;
        this.fireEvent(true);
    }

    public void setUrl(String url) {
        this.trackingUrl = url;
        this.fireEvent(true);
    }

    public void setUser(String user) {
        this.user = user;
        this.fireEvent(true);
    }

    private void fireEvent(boolean isInfoChanged) {
        if (this.listeners != null) {
            Iterator iterator = this.listeners.iterator();

            while (iterator.hasNext()) {
                Listener l = (Listener)iterator.next();
                if (isInfoChanged) {
                    l.infoChanged(this);
                } else {
                    l.stateChanged(this);
                }
            }
        }

    }
}
