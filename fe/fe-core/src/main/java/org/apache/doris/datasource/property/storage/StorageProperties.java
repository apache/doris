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

import lombok.Getter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class StorageProperties extends ConnectionProperties {

    public static final String FS_HDFS_SUPPORT = "fs.hdfs.support";
    public static final String FS_S3_SUPPORT = "fs.s3.support";
    public static final String FS_GCS_SUPPORT = "fs.gcs.support";
    public static final String FS_AZURE_SUPPORT = "fs.azure.support";
    public static final String FS_OSS_SUPPORT = "fs.oss.support";
    public static final String FS_OBS_SUPPORT = "fs.obs.support";
    public static final String FS_COS_SUPPORT = "fs.cos.support";

    public enum Type {
        HDFS,
        S3,
        OSS,
        OBS,
        COS,
        UNKNOWN
    }

    public abstract Map<String, String> getBackendConfigProperties();

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
    public static StorageProperties createPrimary(Map<String, String> origProps) throws UserException {
        for (Function<Map<String, String>, StorageProperties> func : PROVIDERS) {
            StorageProperties p = func.apply(origProps);
            if (p != null) {
                p.initNormalizeAndCheckProps();
                return p;
            }
        }
        throw new RuntimeException("No supported storage type found.");
    }

    private static final List<Function<Map<String, String>, StorageProperties>> PROVIDERS =
            Arrays.asList(
                    props -> (isFsSupport(props, FS_HDFS_SUPPORT)
                            || HdfsProperties.guessIsMe(props)) ? new HdfsProperties(props) : null,
                    props -> (isFsSupport(props, FS_S3_SUPPORT)
                            || S3Properties.guessIsMe(props)) ? new S3Properties(props) : null,
                    props -> (isFsSupport(props, FS_OSS_SUPPORT)
                            || OSSProperties.guessIsMe(props)) ? new OSSProperties(props) : null,
                    props -> (isFsSupport(props, FS_OBS_SUPPORT)
                            || OBSProperties.guessIsMe(props)) ? new OBSProperties(props) : null,
                    props -> (isFsSupport(props, FS_COS_SUPPORT)
                            || COSProperties.guessIsMe(props)) ? new COSProperties(props) : null
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
}
