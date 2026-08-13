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

package org.apache.doris.catalog.authorizer.openfga;

import org.apache.doris.catalog.authorizer.ranger.doris.DorisObjectType;

/**
 * Builds the OpenFGA object string for a Doris object at a given level.
 *
 * <p>An OpenFGA object is written as {@code <type>:<id>}. Doris objects are hierarchical
 * (catalog -> database -> table -> column), so the id encodes the full path with a separator to
 * keep it unique across catalogs and databases, for example {@code table:internal/db1/tbl1}.
 * This mirrors {@code RangerDorisResource}, which sets the same catalog/database/table/column keys
 * on a Ranger resource; here they are flattened into a single OpenFGA object id.
 *
 * <ul>
 *   <li>FirstLevelResource  => Catalog / Resource / WorkloadGroup / Global / StorageVault / ComputeGroup</li>
 *   <li>SecondLevelResource => Database</li>
 *   <li>ThirdLevelResource  => Table</li>
 *   <li>FourthLevelResource => Column</li>
 * </ul>
 */
public class OpenFgaResource {
    public static final String TYPE_GLOBAL = "global";
    public static final String TYPE_CATALOG = "catalog";
    public static final String TYPE_DATABASE = "database";
    public static final String TYPE_TABLE = "table";
    public static final String TYPE_COLUMN = "column";
    public static final String TYPE_RESOURCE = "resource";
    public static final String TYPE_WORKLOAD_GROUP = "workload_group";
    public static final String TYPE_COMPUTE_GROUP = "compute_group";
    public static final String TYPE_STORAGE_VAULT = "storage_vault";

    private final String objectType;
    private final String objectId;
    private final String separator;

    public OpenFgaResource(DorisObjectType objectType, String separator, String firstLevelResource) {
        this(objectType, separator, firstLevelResource, null, null, null);
    }

    public OpenFgaResource(DorisObjectType objectType, String separator, String firstLevelResource,
            String secondLevelResource) {
        this(objectType, separator, firstLevelResource, secondLevelResource, null, null);
    }

    public OpenFgaResource(DorisObjectType objectType, String separator, String firstLevelResource,
            String secondLevelResource, String thirdLevelResource) {
        this(objectType, separator, firstLevelResource, secondLevelResource, thirdLevelResource, null);
    }

    public OpenFgaResource(DorisObjectType objectType, String separator, String firstLevelResource,
            String secondLevelResource, String thirdLevelResource, String fourthLevelResource) {
        this.separator = separator;
        switch (objectType) {
            case GLOBAL:
                this.objectType = TYPE_GLOBAL;
                this.objectId = firstLevelResource;
                break;
            case CATALOG:
                this.objectType = TYPE_CATALOG;
                this.objectId = firstLevelResource;
                break;
            case DATABASE:
                this.objectType = TYPE_DATABASE;
                this.objectId = join(firstLevelResource, secondLevelResource);
                break;
            case TABLE:
                this.objectType = TYPE_TABLE;
                this.objectId = join(firstLevelResource, secondLevelResource, thirdLevelResource);
                break;
            case COLUMN:
                this.objectType = TYPE_COLUMN;
                this.objectId = join(firstLevelResource, secondLevelResource, thirdLevelResource, fourthLevelResource);
                break;
            case RESOURCE:
                this.objectType = TYPE_RESOURCE;
                this.objectId = firstLevelResource;
                break;
            case WORKLOAD_GROUP:
                this.objectType = TYPE_WORKLOAD_GROUP;
                this.objectId = firstLevelResource;
                break;
            case STORAGE_VAULT:
                this.objectType = TYPE_STORAGE_VAULT;
                this.objectId = firstLevelResource;
                break;
            case COMPUTE_GROUP:
                this.objectType = TYPE_COMPUTE_GROUP;
                this.objectId = firstLevelResource;
                break;
            case NONE:
            default:
                this.objectType = null;
                this.objectId = null;
                break;
        }
    }

    private String join(String... parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(parts[i]);
        }
        return sb.toString();
    }

    public String getObjectType() {
        return objectType;
    }

    public String getObjectId() {
        return objectId;
    }

    /**
     * Returns the OpenFGA object string {@code <type>:<id>}, or null if this resource has no
     * OpenFGA representation (an unsupported or NONE object type).
     */
    public String getObjectString() {
        if (objectType == null || objectId == null) {
            return null;
        }
        return objectType + ":" + objectId;
    }

    @Override
    public String toString() {
        return getObjectString();
    }
}
