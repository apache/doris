package org.apache.doris.cdcclient.source.factory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.doris.cdcclient.source.reader.SourceReader;
import org.apache.doris.cdcclient.source.reader.mysql.MySqlSourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SourceReader register. */
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

    public static SourceReader<?, ?> createSourceReader(DataSource source) {
        Supplier<SourceReader<?, ?>> supplier = REGISTRY.get(source);
        if (supplier == null) {
            throw new IllegalArgumentException(
                    "Unsupported SourceReader with datasource : " + source);
        }
        return supplier.get();
    }
}
