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

import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.ConnectorProperty;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * AzureProperties is a specialized configuration class for accessing Azure Blob Storage
 * using an S3-compatible interface.
 *
 * <p>This class extends {@link StorageProperties} and adapts Azure-specific properties
 * to a format that is compatible with the backend engine (BE), which expects configurations
 * similar to Amazon S3. This is necessary because the backend is designed to work with
 * S3-style parameters regardless of the actual cloud provider.
 *
 * <p>Although Azure Blob Storage does not use all of the S3 parameters (e.g., region),
 * this class maps and provides dummy or compatible values to satisfy the expected format.
 * It also tags the provider as "azure" in the final configuration map.
 *
 * <p>The class supports common parameters like access key, secret key, endpoint, and
 * path style access, while also ensuring compatibility with existing S3 processing
 * logic by delegating some functionality to {@code S3PropertyUtils}.
 *
 * <p>Typical usage includes validation of required parameters, transformation to a
 * backend-compatible configuration map, and conversion of URLs to storage paths.
 *
 * <p>Note: This class may evolve as the backend introduces native Azure support
 * or adopts a more flexible configuration model.
 *
 * @see StorageProperties
 * @see S3PropertyUtils
 */
public class AzureProperties extends StorageProperties {
    @Getter
    @ConnectorProperty(names = {"azure.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"},
            description = "The endpoint of S3.")
    protected String endpoint = "";


    @Getter
    @ConnectorProperty(names = {"azure.access_key", "s3.access_key", "AWS_ACCESS_KEY", "ACCESS_KEY", "access_key"},
            description = "The access key of S3.")
    protected String accessKey = "";

    @Getter
    @ConnectorProperty(names = {"azure.secret_key", "s3.secret_key", "AWS_SECRET_KEY", "secret_key"},
            description = "The secret key of S3.")
    protected String secretKey = "";

    @Getter
    @ConnectorProperty(names = {"azure.bucket", "s3.bucket"},
            required = false,
            description = "The container of Azure blob.")
    protected String container = "";

    /**
     * Flag indicating whether to use path-style URLs for the object storage system.
     * This value is optional and can be configured by the user.
     */
    @Setter
    @Getter
    @ConnectorProperty(names = {"use_path_style", "s3.path-style-access"}, required = false,
            description = "Whether to use path style URL for the storage.")
    protected String usePathStyle = "false";
    @ConnectorProperty(names = {"force_parsing_by_standard_uri"}, required = false,
            description = "Whether to use path style URL for the storage.")
    @Getter
    protected String forceParsingByStandardUrl = "false";


    public AzureProperties(Map<String, String> origProps) {
        super(Type.AZURE, origProps);
    }

    private static final String AZURE_ENDPOINT_SUFFIX = ".blob.core.windows.net";

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        //check endpoint
        if (!endpoint.endsWith(AZURE_ENDPOINT_SUFFIX)) {
            throw new IllegalArgumentException(String.format("Endpoint '%s' is not valid. It should end with '%s'.",
                    endpoint, AZURE_ENDPOINT_SUFFIX));
        }
        this.endpoint = formatAzureEndpoint(endpoint);
    }

    public static boolean guessIsMe(Map<String, String> origProps) {
        boolean enable = origProps.containsKey(FS_PROVIDER_KEY)
                && origProps.get(FS_PROVIDER_KEY).equalsIgnoreCase("azure");
        if (enable) {
            return true;
        }
        String value = Stream.of("azure.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT")
                .map(origProps::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
        if (!Strings.isNullOrEmpty(value)) {
            return value.endsWith(AZURE_ENDPOINT_SUFFIX);
        }
        return false;
    }

    @Override
    public Map<String, String> getBackendConfigProperties() {
        Map<String, String> s3Props = new HashMap<>();
        s3Props.put("AWS_ENDPOINT", endpoint);
        s3Props.put("AWS_REGION", "dummy_region");
        s3Props.put("AWS_ACCESS_KEY", accessKey);
        s3Props.put("AWS_SECRET_KEY", secretKey);
        s3Props.put("AWS_NEED_OVERRIDE_ENDPOINT", "true");
        s3Props.put("provider", "azure");
        s3Props.put("use_path_style", usePathStyle);
        return s3Props;
    }

    public static final String AZURE_ENDPOINT_TEMPLATE = "https://%s.blob.core.windows.net";

    private String formatAzureEndpoint(String endpoint) {
        if (Config.force_azure_blob_global_endpoint) {
            return String.format(AZURE_ENDPOINT_TEMPLATE, accessKey);
        }
        if (endpoint.contains("://")) {
            return endpoint;
        }
        return "https://" + endpoint;
    }

    @Override
    public String validateAndNormalizeUri(String url) throws UserException {
        return S3PropertyUtils.validateAndNormalizeUri(url, usePathStyle, forceParsingByStandardUrl);

    }

    @Override
    public String validateAndGetUri(Map<String, String> loadProps) throws UserException {
        return S3PropertyUtils.validateAndGetUri(loadProps);
    }

    @Override
    public String getStorageName() {
        return "Azure";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        // Azure does not require any special Hadoop configuration for S3 compatibility.
        // The properties are already set in the getBackendConfigProperties method.
        // This method will be removed in the future when FileIO is fully implemented.
    }
}
