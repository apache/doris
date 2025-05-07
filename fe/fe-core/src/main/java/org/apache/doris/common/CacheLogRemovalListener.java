package org.apache.doris.common;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CacheLogRemovalListener<K, V> implements RemovalListener<K, V> {
    private static final Logger LOG = LogManager.getLogger(CacheLogRemovalListener.class);
    private String moduleName;

    public CacheLogRemovalListener(String moduleName) {
        this.moduleName = moduleName;
    }

    @Override
    public void onRemoval(@Nullable K k, @Nullable V v, @NonNull RemovalCause removalCause) {
        if (RemovalCause.SIZE == removalCause) {
            LOG.warn("[{}]Key: {} was removed. Cause: {}, Value: {}", moduleName, k, removalCause, v);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}]Key: {} was removed. Cause: {}, Value: {}", moduleName, k, removalCause, v);
            }
        }
    }
}
