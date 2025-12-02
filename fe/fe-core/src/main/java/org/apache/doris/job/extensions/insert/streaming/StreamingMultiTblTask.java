package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.SourceOffsetProvider;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import java.util.Map;

@Log4j2
@Getter
public class StreamingMultiTblTask extends AbstractStreamingTask {

    SourceOffsetProvider offsetProvider;
    Map<String, String> sourceProperties;
    Map<String, String> targetProperties;

    public StreamingMultiTblTask(Long jobId,
                                long taskId,
                                SourceOffsetProvider offsetProvider,
                                Map<String, String> sourceProperties,
                                Map<String, String> targetProperties) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.offsetProvider = offsetProvider;
        this.sourceProperties = sourceProperties;
        this.targetProperties = targetProperties;
    }

    @Override
    public void before() throws Exception {
        log.info("StreamingMultiTblTask before execution.");
        this.runningOffset = offsetProvider.getNextOffset(null, sourceProperties);
    }

    @Override
    public void run() throws JobException {
        log.info("StreamingMultiTblTask run execution.");
    }

    @Override
    public boolean onSuccess() throws JobException {
        log.info("StreamingMultiTblTask onSuccess execution.");
        return false;
    }

    @Override
    public void onFail(String errMsg) throws JobException {

    }

    @Override
    public void cancel(boolean needWaitCancelComplete) {

    }

    @Override
    public void closeOrReleaseResources() {

    }
}
