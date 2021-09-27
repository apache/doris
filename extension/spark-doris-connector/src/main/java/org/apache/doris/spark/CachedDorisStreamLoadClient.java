package org.apache.doris.spark;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.doris.spark.exception.DorisException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author wei.zhao
 * <p>
 * a cached streamload client for each partition
 */
public class CachedDorisStreamLoadClient {
    private static final long cacheExpireTimeout = 30 * 60;
    private static LoadingCache<SparkSettings, DorisStreamLoad> dorisStreamLoadLoadingCache;

    static {
        dorisStreamLoadLoadingCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireTimeout, TimeUnit.SECONDS)
                .removalListener(new RemovalListener<Object, Object>() {
                    @Override
                    public void onRemoval(RemovalNotification<Object, Object> removalNotification) {
                        //do nothing
                    }
                })
                .build(
                        new CacheLoader<SparkSettings, DorisStreamLoad>() {
                            @Override
                            public DorisStreamLoad load(SparkSettings sparkSettings) throws IOException, DorisException {
                                DorisStreamLoad dorisStreamLoad = new DorisStreamLoad(sparkSettings);
                                return dorisStreamLoad;
                            }
                        }
                );
    }

    public static DorisStreamLoad getOrCreate(SparkSettings settings) throws ExecutionException {
        DorisStreamLoad dorisStreamLoad = dorisStreamLoadLoadingCache.get(settings);
        return dorisStreamLoad;
    }
}
