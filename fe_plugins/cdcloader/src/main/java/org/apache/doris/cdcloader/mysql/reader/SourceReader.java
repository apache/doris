package org.apache.doris.cdcloader.mysql.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.doris.cdcloader.mysql.rest.model.JobConfig;
import org.apache.doris.job.extensions.cdc.state.SourceSplit;

import java.util.List;

public interface SourceReader<T,C> {

    /**
     * 初始化，在程序启动的时候调用
     */
    void initialize();

    /**
     * 给Fe提供接口，拉取可读的状态，可选
     *
     * @return
     */
    List<? extends SourceSplit> getSourceSplits(JobConfig config) throws JsonProcessingException;

    /**
     * BE读取数据
     * @param meta
     * @return
     * @throws Exception
     */
    T read(C meta) throws Exception;

    /**
     * 关闭的时候调用
     */
    void close(Long jobId);
}
