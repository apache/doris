package org.apache.doris.datasource.property.metastore;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

public class IcebergPropertiesFactory implements MetastorePropertiesFactory{
    private static final Map<String, Function<Map<String, String>, MetastoreProperties>> REGISTERED_SUBTYPES =
            new HashMap<>();

    static {
        register("glue", IcebergGlueMetaStoreProperties::new);
        //register("rest", HMSProperties::new);
        register("hms", IcebergHMSMetaStoreProperties::new);
        register("filesystem", IcebergFileSystemMetaStoreProperties::new);
        register("s3tables", IcebergS3TablesMetaStoreProperties::new);
    }

    public static void register(String subType, Function<Map<String, String>, MetastoreProperties> constructor) {
        REGISTERED_SUBTYPES.put(subType.toLowerCase(Locale.ROOT), constructor);
    }

    @Override
    public MetastoreProperties create(Map<String, String> props) {
        String subType = props.getOrDefault("iceberg.catalog.type", "default").toLowerCase(Locale.ROOT);
        Function<Map<String, String>, MetastoreProperties> constructor =
                REGISTERED_SUBTYPES.getOrDefault(subType, REGISTERED_SUBTYPES.get("default"));
        MetastoreProperties instance = constructor.apply(props);
        instance.initNormalizeAndCheckProps();
        return instance;
    }
}
