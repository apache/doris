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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.metastore.deltalake.glue.GlueCatalogProperties;
import org.apache.doris.connector.spi.DorisConnectorException;

import com.google.common.io.Closer;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/** Glue resolves registrations. Delta Kernel reads the transaction log using the catalog's storage configuration. */
final class DeltaLakeGlueCatalog implements DeltaLakeTableResolver {
    private static final Set<String> PROVIDER_KEYS =
            Set.of("table_type", "spark.sql.sources.provider", "classification");
    private static final String SYMLINK_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat";

    private final String catalogId;
    private final Closer resources = Closer.create();
    private final GlueClient glue;

    DeltaLakeGlueCatalog(GlueCatalogProperties properties) {
        this(properties, GlueClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()),
                StsClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder()));
    }

    DeltaLakeGlueCatalog(GlueCatalogProperties properties, GlueClientBuilder glueBuilder, StsClientBuilder stsBuilder) {
        catalogId = properties.catalogId();
        Region region = Region.of(properties.region());
        try {
            AwsCredentialsProvider credentials = sourceCredentials(properties);
            if (properties.roleArn() != null) {
                StsClient sts = own(stsBuilder.region(region).credentialsProvider(credentials).build());
                credentials = own(StsAssumeRoleCredentialsProvider.builder().stsClient(sts)
                        .refreshRequest(request -> request.roleArn(properties.roleArn())
                                .roleSessionName("doris-deltalake-glue").externalId(properties.externalId()))
                        .build());
            }
            URI endpoint = properties.endpoint();
            if (endpoint != null) {
                glueBuilder.endpointOverride(endpoint);
            }
            glue = own(glueBuilder.region(region).credentialsProvider(credentials).build());
        } catch (RuntimeException | Error failure) {
            try {
                resources.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private AwsCredentialsProvider sourceCredentials(GlueCatalogProperties properties) {
        if (properties.accessKey() == null) {
            return own(DefaultCredentialsProvider.builder().build());
        }
        AwsCredentials credentials = properties.sessionToken() == null
                ? AwsBasicCredentials.create(properties.accessKey(), properties.secretKey())
                : AwsSessionCredentials.create(properties.accessKey(), properties.secretKey(),
                        properties.sessionToken());
        return StaticCredentialsProvider.create(credentials);
    }

    private <T extends SdkAutoCloseable> T own(T resource) {
        resources.register(resource::close);
        return resource;
    }

    @Override
    public List<String> listDatabases() {
        List<String> names = new ArrayList<>();
        glue.getDatabasesPaginator(request -> request.catalogId(catalogId))
                .forEach(page -> page.databaseList().forEach(database -> names.add(database.name())));
        return names;
    }

    @Override
    public List<String> listTables(String database) {
        List<String> names = new ArrayList<>();
        try {
            glue.getTablesPaginator(request -> request.catalogId(catalogId).databaseName(database))
                    .forEach(page -> page.tableList().stream().filter(DeltaLakeGlueCatalog::isDelta)
                            .map(Table::name).forEach(names::add));
        } catch (EntityNotFoundException e) {
            return List.of();
        }
        return names;
    }

    @Override
    public Optional<DeltaLakeTableReference> tableLocation(String database, String table) {
        Table entry;
        try {
            entry = glue.getTable(request -> request.catalogId(catalogId).databaseName(database).name(table)).table();
        } catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        if (!isDelta(entry)) {
            return Optional.empty();
        }
        String location = tableRoot(entry.storageDescriptor(), registeredTableName(database, table));
        return Optional.of(new DeltaLakeTableReference(location, null, false));
    }

    private static boolean isDelta(Table table) {
        return table.parameters().entrySet().stream().anyMatch(entry ->
                PROVIDER_KEYS.contains(entry.getKey().toLowerCase(Locale.ROOT))
                        && "delta".equalsIgnoreCase(StringUtils.trim(entry.getValue())));
    }

    private static String tableRoot(StorageDescriptor storage, String name) {
        String location = storage == null ? null : StringUtils.trimToNull(storage.location());
        if (storage != null && missingOrPlaceholder(location) && storage.serdeInfo() != null) {
            location = StringUtils.trimToNull(storage.serdeInfo().parameters().get("path"));
        }
        if (missingOrPlaceholder(location)) {
            throw new DorisConnectorException("Missing storage location for Delta Glue table " + name);
        }
        Path path;
        try {
            path = new Path(location);
        } catch (IllegalArgumentException e) {
            throw new DorisConnectorException("Invalid storage location for Delta Glue table " + name, e);
        }
        URI uri = path.toUri();
        if ((!path.isAbsolute() && uri.getAuthority() == null)
                || (("s3".equalsIgnoreCase(uri.getScheme()) || "s3a".equalsIgnoreCase(uri.getScheme()))
                        && StringUtils.isBlank(uri.getAuthority()))) {
            throw new DorisConnectorException("Invalid storage location for Delta Glue table " + name);
        }
        if (SYMLINK_INPUT_FORMAT.equals(storage.inputFormat())
                || (uri.getPath() + "/").contains("/_symlink_format_manifest/")) {
            throw new DorisConnectorException("Symlink manifest is not a native Delta table location for " + name);
        }
        return location;
    }

    private static boolean missingOrPlaceholder(String location) {
        // Spark's Hive catalog can put a synthetic warehouse path here and the real root in SerDe.
        return location == null || StringUtils.stripEnd(location, "/").endsWith("-__PLACEHOLDER__");
    }

    @Override
    public String registeredTableName(String database, String table) {
        // Use the source identifier, including the requested Glue account when supplied.
        return (catalogId == null ? "" : catalogId + ".") + database + "." + table;
    }

    @Override
    public void close() throws IOException {
        resources.close();
    }
}
