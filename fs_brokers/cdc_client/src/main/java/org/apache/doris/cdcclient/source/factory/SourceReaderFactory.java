package org.apache.doris.cdcclient.source.factory;

import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.mysql.MySqlSourceReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * SourceReader 注册中心，支持在启动阶段按需扩展数据源。
 */
public final class SourceReaderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SourceReaderFactory.class);
    private static final Map<DataSource, Supplier<SourceReader<?, ?>>> REGISTRY =
            new ConcurrentHashMap<>();

    static {
        register(DataSource.MYSQL, MySqlSourceReader::new);
    }

    private SourceReaderFactory() {}

    public static void register(DataSource source, Supplier<SourceReader<?, ?>> supplier) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(supplier, "supplier");
        REGISTRY.put(source, supplier);
        LOG.info("Registered SourceReader provider for {}", source);
    }

    public static void unregister(DataSource source) {
        if (source == null) {
            return;
        }
        REGISTRY.remove(source);
        LOG.info("Unregistered SourceReader provider for {}", source);
    }

    public static boolean supports(DataSource source) {
        return source != null && REGISTRY.containsKey(source);
    }

    public static Set<DataSource> supportedDataSources() {
        return Collections.unmodifiableSet(REGISTRY.keySet());
    }

    public static SourceReader<?, ?> createSourceReader(DataSource source) {
        Supplier<SourceReader<?, ?>> supplier = REGISTRY.get(source);
        if (supplier == null) {
            throw new IllegalArgumentException(
                    "Unsupported SourceReader with datasource : " + source);
        }
        return supplier.get();
    }
}
