// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.property.storage;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectionProperties;
import org.apache.doris.datasource.property.ConnectorProperty;
import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class StorageProperties extends ConnectionProperties {

    public static final String FS_HDFS_SUPPORT = "fs.hdfs.support";
    public static final String FS_S3_SUPPORT = "fs.s3.support";
    public static final String FS_GCS_SUPPORT = "fs.gcs.support";
    public static final String FS_MINIO_SUPPORT = "fs.minio.support";
    public static final String FS_BROKER_SUPPORT = "fs.broker.support";
    public static final String FS_AZURE_SUPPORT = "fs.azure.support";
    public static final String FS_OSS_SUPPORT = "fs.oss.support";
    public static final String FS_OBS_SUPPORT = "fs.obs.support";
    public static final String FS_COS_SUPPORT = "fs.cos.support";
    public static final String FS_OSS_HDFS_SUPPORT = "fs.oss-hdfs.support";
    public static final String FS_LOCAL_SUPPORT = "fs.local.support";
    public static final String DEPRECATED_OSS_HDFS_SUPPORT = "oss.hdfs.enabled";

    public static final String FS_PROVIDER_KEY = "provider";

    public enum Type {
        HDFS,
        S3,
        OSS,
        OBS,
        COS,
        OSS_HDFS,
        MINIO,
        AZURE,
        BROKER,
        LOCAL,
        UNKNOWN
    }

    public abstract Map<String, String> getBackendConfigProperties();

    /**
     * Hadoop storage configuration used for interacting with HDFS-based systems.
     * <p>
     * Currently, some underlying APIs in Hive and Iceberg still rely on the HDFS protocol directly.
     * Because of this, we must introduce an additional storage layer conversion here to adapt
     * our system's storage abstraction to the HDFS protocol.
     * <p>
     * In the future, once we have unified the storage access layer by implementing our own
     * FileIO abstraction (a custom, unified interface for file system access),
     * this conversion layer will no longer be necessary. The FileIO abstraction
     * will provide seamless and consistent access to different storage backends,
     * eliminating the need to rely on HDFS protocol specifics.
     * <p>
     * This approach will simplify the integration and improve maintainability
     * by standardizing the way storage systems are accessed.
     */
    @Getter
    public Configuration hadoopStorageConfig;

    /**
     * Get backend configuration properties with optional runtime properties.
     * This method allows passing runtime properties (like vended credentials)
     * that should be merged with the base configuration.
     *
     * @param runtimeProperties additional runtime properties to merge, can be null
     * @return Map of backend properties including runtime properties
     */
    public Map<String, String> getBackendConfigProperties(Map<String, String> runtimeProperties) {
        Map<String, String> properties = new HashMap<>(getBackendConfigProperties());
        if (runtimeProperties != null && !runtimeProperties.isEmpty()) {
            properties.putAll(runtimeProperties);
        }
        return properties;
    }

    @Getter
    protected Type type;


    /**
     * Creates a list of StorageProperties instances based on the provided properties.
     * <p>
     * This method iterates through the list of supported storage types and creates an instance
     * for each supported type. If no supported type is found, an HDFSProperties instance is added
     * by default.
     *
     * @param origProps the original properties map to create the StorageProperties instances
     * @return a list of StorageProperties instances for all supported storage types
     */
    public static List<StorageProperties> createAll(Map<String, String> origProps) throws UserException {
        List<StorageProperties> result = new ArrayList<>();
        for (Function<Map<String, String>, StorageProperties> func : PROVIDERS) {
            StorageProperties p = func.apply(origProps);
            if (p != null) {
                result.add(p);
            }
        }
        if (result.stream().noneMatch(HdfsProperties.class::isInstance)) {
            result.add(new HdfsProperties(origProps));
        }

        for (StorageProperties storageProperties : result) {
            storageProperties.initNormalizeAndCheckProps();
            storageProperties.initializeHadoopStorageConfig();
        }
        return result;
    }

    /**
     * Creates a primary StorageProperties instance based on the provided properties.
     * <p>
     * This method iterates through the list of supported storage types and returns the first
     * matching StorageProperties instance. If no supported type is found, an exception is thrown.
     *
     * @param origProps the original properties map to create the StorageProperties instance
     * @return a StorageProperties instance for the primary storage type
     * @throws RuntimeException if no supported storage type is found
     */
    public static StorageProperties createPrimary(Map<String, String> origProps) {
        for (Function<Map<String, String>, StorageProperties> func : PROVIDERS) {
            StorageProperties p = func.apply(origProps);
            if (p != null) {
                p.initNormalizeAndCheckProps();
                p.initializeHadoopStorageConfig();
                return p;
            }
        }
        throw new StoragePropertiesException("No supported storage type found. Please check your configuration.");
    }

    private static final List<Function<Map<String, String>, StorageProperties>> PROVIDERS =
            Arrays.asList(
                    props -> (isFsSupport(props, FS_HDFS_SUPPORT)
                            || HdfsProperties.guessIsMe(props)) ? new HdfsProperties(props) : null,
                    props -> ((isFsSupport(props, FS_OSS_HDFS_SUPPORT)
                            || isFsSupport(props, DEPRECATED_OSS_HDFS_SUPPORT))
                            || OSSHdfsProperties.guessIsMe(props)) ? new OSSHdfsProperties(props) : null,
                    props -> (isFsSupport(props, FS_S3_SUPPORT)
                            || S3Properties.guessIsMe(props)) ? new S3Properties(props) : null,
                    props -> (isFsSupport(props, FS_OSS_SUPPORT)
                            || OSSProperties.guessIsMe(props)) ? new OSSProperties(props) : null,
                    props -> (isFsSupport(props, FS_OBS_SUPPORT)
                            || OBSProperties.guessIsMe(props)) ? new OBSProperties(props) : null,
                    props -> (isFsSupport(props, FS_COS_SUPPORT)
                            || COSProperties.guessIsMe(props)) ? new COSProperties(props) : null,
                    props -> (isFsSupport(props, FS_AZURE_SUPPORT)
                            || AzureProperties.guessIsMe(props)) ? new AzureProperties(props) : null,
                    props -> (isFsSupport(props, FS_MINIO_SUPPORT)
                            || MinioProperties.guessIsMe(props)) ? new MinioProperties(props) : null,
                    props -> (isFsSupport(props, FS_BROKER_SUPPORT)
                            || BrokerProperties.guessIsMe(props)) ? new BrokerProperties(props) : null,
                    props -> (isFsSupport(props, FS_LOCAL_SUPPORT)
                            || LocalProperties.guessIsMe(props)) ? new LocalProperties(props) : null
            );

    protected StorageProperties(Type type, Map<String, String> origProps) {
        super(origProps);
        this.type = type;
    }

    private static boolean isFsSupport(Map<String, String> origProps, String fsEnable) {
        return origProps.getOrDefault(fsEnable, "false").equalsIgnoreCase("true");
    }

    protected static boolean checkIdentifierKey(Map<String, String> origProps, List<Field> fields) {
        for (Field field : fields) {
            field.setAccessible(true);
            ConnectorProperty annotation = field.getAnnotation(ConnectorProperty.class);
            for (String key : annotation.names()) {
                if (origProps.containsKey(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Validates the given URL string and returns a normalized URI in the format: scheme://authority/path.
     * <p>
     * This method checks that the input is non-empty, the scheme is present and supported (e.g., hdfs, viewfs),
     * and converts it into a canonical URI string.
     *
     * @param url the raw URL string to validate and normalize
     * @return a normalized URI string with validated scheme and authority
     * @throws UserException if the URL is empty, lacks a valid scheme, or contains an unsupported scheme
     */
    public abstract String validateAndNormalizeUri(String url) throws UserException;

    /**
     * Extracts the URI string from the provided properties map, validates it, and returns the normalized URI.
     * <p>
     * This method checks that the 'uri' key exists in the property map, retrieves the value,
     * and then delegates to {@link #validateAndNormalizeUri(String)} for further validation and normalization.
     *
     * @param loadProps the map containing load-related properties, including the URI under the key 'uri'
     * @return a normalized and validated URI string
     * @throws UserException if the 'uri' property is missing, empty, or invalid
     */
    public abstract String validateAndGetUri(Map<String, String> loadProps) throws UserException;

    public abstract String getStorageName();

    public abstract void initializeHadoopStorageConfig();
}
