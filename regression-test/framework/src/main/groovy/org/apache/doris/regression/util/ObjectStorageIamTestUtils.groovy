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

package org.apache.doris.regression.util

import java.util.Locale

/**
 * Builds the IAM authentication matrix shared by the object_storage_iam_p0 suites.
 *
 * <p>One regression-test invocation selects exactly one cloud with
 * {@code objectStorageIamProvider=AWS} or {@code objectStorageIamProvider=GCP}. Common connection
 * settings are reused by both providers. Leaving {@code objectStorageIamProvider} empty disables
 * the shared cases; AWS-only suites also use {@link #isProvider(Map, String)} to skip themselves
 * during a GCP run.
 *
 * <p>AWS role example in regression-conf.groovy:
 * <pre>
 * objectStorageIamProvider="AWS"
 * objectStorageIamEndpoint="https://s3.us-east-1.amazonaws.com"
 * objectStorageIamRegion="us-east-1"
 * objectStorageIamBucket="my-bucket"
 * objectStorageIamPrefix="regression/object_storage_iam_p0"
 * objectStorageIamDataPath="regression/tpch/sf0.01/customer.csv.gz"
 * objectStorageIamAwsRoleArn="arn:aws:iam::123456789012:role/my-role"
 * objectStorageIamAwsExternalId="optional-external-id"
 * </pre>
 *
 * <p>GCP example covering DEFAULT, COMPUTE_ENGINE, and service-account impersonation:
 * <pre>
 * objectStorageIamProvider="GCP"
 * objectStorageIamEndpoint="https://storage.googleapis.com"
 * objectStorageIamRegion="us-east1"
 * objectStorageIamBucket="my-bucket"
 * objectStorageIamPrefix="regression/object_storage_iam_p0"
 * objectStorageIamDataPath="regression/tpch/sf0.01/customer.csv.gz"
 * objectStorageIamGcpCredentialProviderTypes="DEFAULT,COMPUTE_ENGINE"
 * objectStorageIamGcpImpersonationServiceAccount="target@project.iam.gserviceaccount.com"
 * </pre>
 *
 * <p>The comma-separated GCP provider list controls which direct credential cases run. DEFAULT also
 * tests omitted authentication properties against the same private fixture. If both the provider
 * list and impersonation account are empty, only the omitted-authentication case runs. A non-empty
 * impersonation service account adds one more COMPUTE_ENGINE case. Each shared suite iterates over
 * the resulting {@code authCases}. The test selector uses AWS/GCP, while SQL properties use the
 * public S3/GCP provider values.
 */
class ObjectStorageIamTestUtils {
    private static final List<String> PROVIDERS = ["AWS", "GCP"]
    private static final List<String> GCP_CREDENTIAL_PROVIDER_TYPES = ["DEFAULT", "COMPUTE_ENGINE"]
    private static final List<String> REQUIRED_CONFIGS = [
            "objectStorageIamEndpoint",
            "objectStorageIamRegion",
            "objectStorageIamBucket",
            "objectStorageIamPrefix",
            "objectStorageIamDataPath"
    ]

    static Map getConfig(Map configs) {
        String provider = value(configs, "objectStorageIamProvider")?.toUpperCase(Locale.ROOT)
        if (provider == null || provider.isEmpty()) {
            return null
        }
        if (!PROVIDERS.contains(provider)) {
            throw new IllegalArgumentException("Unsupported objectStorageIamProvider: ${provider}; "
                    + "supported values are ${PROVIDERS}")
        }

        List<String> missingConfigs = REQUIRED_CONFIGS.findAll { isBlank(value(configs, it)) }
        if (!missingConfigs.isEmpty()) {
            throw new IllegalArgumentException("Missing object storage IAM configs: ${missingConfigs}")
        }

        List<Map> authCases = provider == "AWS"
                ? getAwsAuthCases(configs)
                : getGcpAuthCases(configs)
        String endpointProperty = provider == "AWS" ? "s3.endpoint" : "gs.endpoint"
        String propertyProvider = provider == "AWS" ? "S3" : "GCP"
        String endpoint = value(configs, "objectStorageIamEndpoint")
        String region = value(configs, "objectStorageIamRegion")
        authCases.each { authCase ->
            Map<String, String> storageProperties = new LinkedHashMap<>()
            storageProperties.put("provider", propertyProvider)
            storageProperties.put(endpointProperty, endpoint)
            storageProperties.put("s3.region", region)
            storageProperties.putAll(authCase.properties)
            authCase.storageProperties = storageProperties
            authCase.storageSqlProperties = toSqlProperties(storageProperties)
        }
        return [
                provider: provider,
                propertyProvider: propertyProvider,
                scheme: provider == "AWS" ? "s3" : "gs",
                endpoint: endpoint,
                region: region,
                bucket: value(configs, "objectStorageIamBucket"),
                prefix: value(configs, "objectStorageIamPrefix"),
                dataPath: value(configs, "objectStorageIamDataPath"),
                roleArn: value(configs, "objectStorageIamAwsRoleArn"),
                externalId: value(configs, "objectStorageIamAwsExternalId"),
                authCases: authCases
        ]
    }

    static boolean isProvider(Map configs, String expectedProvider) {
        String provider = value(configs, "objectStorageIamProvider")
        return provider != null && provider.equalsIgnoreCase(expectedProvider)
    }

    private static List<Map> getAwsAuthCases(Map configs) {
        String roleArn = value(configs, "objectStorageIamAwsRoleArn")
        if (roleArn == null || roleArn.isEmpty()) {
            throw new IllegalArgumentException("objectStorageIamAwsRoleArn must be configured for AWS")
        }

        Map<String, String> properties = ["s3.role_arn": roleArn]
        String externalId = value(configs, "objectStorageIamAwsExternalId")
        if (externalId != null && !externalId.isEmpty()) {
            properties.put("s3.external_id", externalId)
        }
        return [authCase("aws_role", properties)]
    }

    private static List<Map> getGcpAuthCases(Map configs) {
        String configuredTypes = value(configs, "objectStorageIamGcpCredentialProviderTypes")
        List<String> providerTypes = (configuredTypes ?: "").split(",")
                .collect { it.trim().toUpperCase(Locale.ROOT) }
                .findAll { !it.isEmpty() }
                .unique()
        List<String> unsupportedTypes = providerTypes.findAll {
            !GCP_CREDENTIAL_PROVIDER_TYPES.contains(it)
        }
        if (!unsupportedTypes.isEmpty()) {
            throw new IllegalArgumentException(
                    "Unsupported objectStorageIamGcpCredentialProviderTypes: ${unsupportedTypes}; "
                    + "supported values are ${GCP_CREDENTIAL_PROVIDER_TYPES}")
        }

        List<Map> cases = providerTypes.collect { providerType ->
            authCase("gcp_${providerType.toLowerCase(Locale.ROOT)}",
                    ["gs.credential_provider_type": providerType])
        }
        String serviceAccount = value(configs, "objectStorageIamGcpImpersonationServiceAccount")
        if (serviceAccount != null && !serviceAccount.isEmpty()) {
            cases.add(authCase("gcp_impersonation", [
                    "gs.credential_provider_type": "COMPUTE_ENGINE",
                    "gs.impersonation_service_account": serviceAccount
            ]))
        }
        if (providerTypes.contains("DEFAULT") || cases.isEmpty()) {
            // Exercise the VM-attached service account without either native authentication property.
            // Shared TVF, Load, Outfile, Export, Resource, Catalog and Vault suites all consume this case.
            cases.add(0, authCase("gcp_default_omitted", [:]))
        }
        return cases
    }

    private static Map authCase(String name, Map<String, String> properties) {
        return [
                name: name,
                properties: properties,
                authProperties: toSqlProperties(properties)
        ]
    }

    static String toSqlProperties(Map<String, String> properties) {
        return properties.collect { key, propertyValue ->
            "\"${key}\" = \"${propertyValue}\""
        }.join(",\n")
    }

    private static String value(Map configs, String key) {
        return configs.get(key)?.toString()?.trim()
    }

    private static boolean isBlank(String value) {
        return value == null || value.isEmpty()
    }
}
