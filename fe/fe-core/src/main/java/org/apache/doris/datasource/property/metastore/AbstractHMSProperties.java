package org.apache.doris.datasource.property.metastore;

import lombok.Getter;
import org.apache.doris.common.Config;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.security.authentication.HadoopSimpleAuthenticator;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

public abstract class AbstractHMSProperties extends MetastoreProperties {

    @Getter
    protected HiveConf hiveConf;

    @Getter
    protected HadoopAuthenticator hdfsAuthenticator;


    @Getter
    protected boolean hmsEventsIncrementalSyncEnabled = Config.enable_hms_events_incremental_sync;

    @Getter
    protected int hmsEventsBatchSizePerRpc = Config.hms_events_batch_size_per_rpc;


    /**
     * Base constructor for subclasses to initialize the common state.
     *
     * @param type      metastore type
     * @param origProps original configuration
     */
    protected AbstractHMSProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }
}
