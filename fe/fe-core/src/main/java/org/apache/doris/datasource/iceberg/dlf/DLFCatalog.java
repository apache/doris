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

package org.apache.doris.datasource.iceberg.dlf;

import org.apache.doris.common.credentials.CloudCredential;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.datasource.iceberg.dlf.client.DLFCachedClientPool;
import org.apache.doris.datasource.iceberg.hive.HiveCompatibleCatalog;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.datasource.property.constants.S3Properties;

import org.apache.hadoop.fs.aliyun.oss.Constants;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DLFCatalog extends HiveCompatibleCatalog {

    @Override
    public void initialize(String name, Map<String, String> properties) {
        super.initialize(name, initializeFileIO(properties), new DLFCachedClientPool(this.conf, properties));
    }

    protected FileIO initializeFileIO(Map<String, String> properties) {
        // read from converted properties or default by old s3 aws properties
        String endpoint = properties.getOrDefault(Constants.ENDPOINT_KEY, properties.get(S3Properties.Env.ENDPOINT));
        CloudCredential credential = new CloudCredential();
        credential.setAccessKey(properties.getOrDefault(OssProperties.ACCESS_KEY,
                properties.get(S3Properties.Env.ACCESS_KEY)));
        credential.setSecretKey(properties.getOrDefault(OssProperties.SECRET_KEY,
                properties.get(S3Properties.Env.SECRET_KEY)));
        if (properties.containsKey(OssProperties.SESSION_TOKEN)
                || properties.containsKey(S3Properties.Env.TOKEN)) {
            credential.setSessionToken(properties.getOrDefault(OssProperties.SESSION_TOKEN,
                    properties.get(S3Properties.Env.TOKEN)));
        }
        String region = properties.getOrDefault(OssProperties.REGION, properties.get(S3Properties.Env.REGION));
        boolean isUsePathStyle = properties.getOrDefault(PropertyConverter.USE_PATH_STYLE, "false")
                .equalsIgnoreCase("true");
        // s3 file io just supports s3-like endpoint
        String s3Endpoint = endpoint.replace(region, "s3." + region);
        URI endpointUri = URI.create(s3Endpoint);
        FileIO io = new S3FileIO(() -> S3Util.buildS3Client(endpointUri, region, credential, isUsePathStyle));
        io.initialize(properties);
        return io;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        return null;
    }

    protected boolean isValidNamespace(Namespace namespace) {
        return namespace.levels().length != 1;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        if (isValidNamespace(namespace)) {
            throw new NoSuchTableException("Invalid namespace: %s", namespace);
        }
        String dbName = namespace.level(0);
        try {
            return clients.run(client -> client.getAllTables(dbName))
                .stream()
                .map(tbl -> TableIdentifier.of(dbName, tbl))
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
        throw new UnsupportedOperationException(
            "Cannot drop table " + tableIdentifier + " : dropTable is not supported");
    }

    @Override
    public void renameTable(TableIdentifier sourceTbl, TableIdentifier targetTbl) {
        throw new UnsupportedOperationException(
            "Cannot rename table " + sourceTbl + " : renameTable is not supported");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> props) {
        throw new UnsupportedOperationException(
            "Cannot create namespace " + namespace + " : createNamespace is not supported");
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        if (isValidNamespace(namespace) && !namespace.isEmpty()) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
        if (!namespace.isEmpty()) {
            return new ArrayList<>();
        }
        List<Namespace> namespaces = new ArrayList<>();
        List<String> databases;
        try {
            databases = clients.run(client -> client.getAllDatabases());
            for (String database : databases) {
                namespaces.add(Namespace.of(database));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return namespaces;
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        if (isValidNamespace(namespace)) {
            throw new NoSuchTableException("Invalid namespace: %s", namespace);
        }
        String dbName = namespace.level(0);
        try {
            return clients.run(client -> client.getDatabase(dbName)).getParameters();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        throw new UnsupportedOperationException(
            "Cannot drop namespace " + namespace + " : dropNamespace is not supported");
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> props) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException(
            "Cannot set namespace properties " + namespace + " : setProperties is not supported");
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> pNames) throws NoSuchNamespaceException {
        throw new UnsupportedOperationException(
            "Cannot remove properties " + namespace + " : removeProperties is not supported");
    }
}
