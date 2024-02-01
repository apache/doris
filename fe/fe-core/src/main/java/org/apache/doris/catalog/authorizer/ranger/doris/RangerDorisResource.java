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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

public class RangerDorisResource extends RangerAccessResourceImpl {
    public static final String KEY_CATALOG = "catalog";
    public static final String KEY_DATABASE = "database";
    public static final String KEY_TABLE = "table";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_RESOURCE = "resource";
    public static final String KEY_WORKLOAD_GROUP = "workload_group";

    // FirstLevelResource => Catalog / Resource / WorkloadGroup
    // SecondLevelResource => Database
    // ThirdLevelResource => Table
    // FourthLevelResource => Column
    public RangerDorisResource(DorisObjectType objectType, String firstLevelResource) {
        this(objectType, firstLevelResource, null, null, null);
    }

    public RangerDorisResource(DorisObjectType objectType, String firstLevelResource, String secondLevelResource) {
        this(objectType, firstLevelResource, secondLevelResource, null, null);
    }

    public RangerDorisResource(DorisObjectType objectType, String firstLevelResource, String secondLevelResource,
            String thirdLevelResource) {
        this(objectType, firstLevelResource, secondLevelResource, thirdLevelResource, null);
    }

    public RangerDorisResource(DorisObjectType objectType, String firstLevelResource, String secondLevelResource,
                              String thirdLevelResource, String fourthLevelResource) {
        // set essential info according to objectType
        switch (objectType) {
            case CATALOG:
                setValue(KEY_CATALOG, firstLevelResource);
                break;
            case DATABASE:
                setValue(KEY_CATALOG, firstLevelResource);
                setValue(KEY_DATABASE, secondLevelResource);
                break;
            case TABLE:
                setValue(KEY_CATALOG, firstLevelResource);
                setValue(KEY_DATABASE, secondLevelResource);
                setValue(KEY_TABLE, thirdLevelResource);
                break;
            case COLUMN:
                setValue(KEY_CATALOG, firstLevelResource);
                setValue(KEY_DATABASE, secondLevelResource);
                setValue(KEY_TABLE, thirdLevelResource);
                setValue(KEY_COLUMN, fourthLevelResource);
                break;
            case RESOURCE:
                setValue(KEY_RESOURCE, firstLevelResource);
                break;
            case WORKLOAD_GROUP:
                setValue(KEY_WORKLOAD_GROUP, firstLevelResource);
                break;
            case NONE:
            default:
                break;
        }
    }
}
