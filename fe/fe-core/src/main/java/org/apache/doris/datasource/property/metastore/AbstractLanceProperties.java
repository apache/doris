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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.StringUtils;
import org.lance.namespace.LanceNamespace;

import java.util.Map;

/** Common properties shared by Lance filesystem and REST namespace catalogs. */
public abstract class AbstractLanceProperties extends MetastoreProperties {
    public static final String LANCE_CATALOG_TYPE = "lance.catalog.type";
    public static final String LANCE_FILESYSTEM = "filesystem";
    public static final String LANCE_REST = "rest";
    public static final String NAMESPACE_PARENT = "lance.namespace.parent";
    public static final String NAMESPACE_DELIMITER = "lance.namespace.delimiter";
    public static final String ROOT_DATABASE = "lance.namespace.root_database";

    public static final String DEFAULT_DELIMITER = "$";
    public static final String DEFAULT_ROOT_DATABASE = "default";

    @ConnectorProperty(
            names = {NAMESPACE_PARENT},
            required = false,
            description = "The optional Lance namespace under which Doris databases are exposed.")
    private String namespaceParent = "";

    @ConnectorProperty(
            names = {NAMESPACE_DELIMITER},
            required = false,
            description = "The delimiter used in an escaped parent namespace. Default: $."
    )
    private String namespaceDelimiter = DEFAULT_DELIMITER;

    @ConnectorProperty(
            names = {ROOT_DATABASE},
            required = false,
            description = "The Doris database name representing the catalog's root namespace. Default: default."
    )
    private String rootDatabase = DEFAULT_ROOT_DATABASE;

    protected AbstractLanceProperties(Map<String, String> props) {
        super(Type.LANCE, props);
    }

    @Override
    public final void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        // Preserve namespace whitespace and explicit empty values because both are meaningful
        // to the escaping rules and must be validated rather than silently replaced by defaults.
        namespaceParent = origProps.getOrDefault(NAMESPACE_PARENT, "");
        namespaceDelimiter = origProps.getOrDefault(NAMESPACE_DELIMITER, DEFAULT_DELIMITER);
        rootDatabase = origProps.getOrDefault(ROOT_DATABASE, DEFAULT_ROOT_DATABASE);
        validateCommonProperties();
        validateCatalogProperties();
    }

    public abstract String getLanceCatalogType();

    public abstract LanceNamespace createNamespace(
            BufferAllocator allocator, Map<String, String> javaStorageOptions);

    /**
     * The URL whose storage the namespace client reads itself, which decides how its options have
     * to be spelled. Empty when it reads none: a REST namespace is reached over HTTP and ignores
     * the options entirely.
     */
    public String getNamespaceStorageUri() {
        return "";
    }

    protected abstract void validateCatalogProperties();

    public String getNamespaceParent() {
        return namespaceParent;
    }

    public String getNamespaceDelimiter() {
        return namespaceDelimiter;
    }

    public String getRootDatabase() {
        return rootDatabase;
    }

    private void validateCommonProperties() {
        if (namespaceDelimiter.isEmpty() || namespaceDelimiter.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("Property '" + NAMESPACE_DELIMITER
                    + "' cannot be empty or contain the escape character '\\'");
        }
        if (StringUtils.isBlank(rootDatabase)) {
            throw new IllegalArgumentException("Property '" + ROOT_DATABASE + "' must be non-empty");
        }
    }
}
