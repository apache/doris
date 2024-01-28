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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

public class RangerHiveResource extends RangerAccessResourceImpl {
    public static final String KEY_DATABASE = "database";
    public static final String KEY_TABLE = "table";
    public static final String KEY_UDF = "udf";
    public static final String KEY_COLUMN = "column";
    private HiveObjectType objectType;

    //FirstLevelResource => Database
    //SecondLevelResource => Table or UDF
    //ThirdLevelResource => column
    public RangerHiveResource(HiveObjectType objectType, String firstLevelResource) {
        this(objectType, firstLevelResource, null, null);
    }

    public RangerHiveResource(HiveObjectType objectType, String firstLevelResource, String secondLevelResource) {
        this(objectType, firstLevelResource, secondLevelResource, null);
    }

    public RangerHiveResource(HiveObjectType objectType, String firstLevelResource, String secondLevelResource,
                              String thirdLevelResource) {
        this.objectType = objectType;

        // set essential info according to objectType
        switch (objectType) {
            case DATABASE:
                setValue(KEY_DATABASE, firstLevelResource);
                break;

            case FUNCTION:
                if (firstLevelResource == null) {
                    firstLevelResource = "";
                }
                setValue(KEY_DATABASE, firstLevelResource);
                setValue(KEY_UDF, secondLevelResource);
                break;

            case COLUMN:
                setValue(KEY_DATABASE, firstLevelResource);
                setValue(KEY_TABLE, secondLevelResource);
                setValue(KEY_COLUMN, thirdLevelResource);
                break;

            case TABLE:
            case VIEW:
            case INDEX:
                setValue(KEY_DATABASE, firstLevelResource);
                setValue(KEY_TABLE, secondLevelResource);
                break;

            case NONE:
            default:
                break;
        }
    }

    public HiveObjectType getObjectType() {
        return objectType;
    }

    public String getDatabase() {
        return (String) getValue(KEY_DATABASE);
    }

    public String getTable() {
        return (String) getValue(KEY_TABLE);
    }

    public String getUdf() {
        return (String) getValue(KEY_UDF);
    }

    public String getColumn() {
        return (String) getValue(KEY_COLUMN);
    }
}
