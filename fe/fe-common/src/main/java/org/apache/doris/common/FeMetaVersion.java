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

package org.apache.doris.common;

public final class FeMetaVersion {
    // for max query instance
    public static final int VERSION_100 = 100;
    // add errorRowsAfterResumed to distinguish totalErrorRows and currentErrorRows even if the job is paused.
    public static final int VERSION_101 = 101;
    // add data encrypt
    public static final int VERSION_102 = 102;
    // support sync job
    public static final int VERSION_103 = 103;
    // add sql block rule to deny specified sql
    public static final int VERSION_104 = 104;
    // change replica to replica allocation
    public static final int VERSION_105 = 105;
    // add ldap info
    public static final int VERSION_106 = 106;
    // support stream load 2PC
    public static final int VERSION_107 = 107;
    // add storage_cold_medium and remote_storage_resource_name in DataProperty
    public static final int VERSION_108 = 108;
    // note: when increment meta version, should assign the latest version to VERSION_CURRENT
    public static final int VERSION_CURRENT = VERSION_108;

    // all logs meta version should >= the minimum version, so that we could remove many if clause, for example
    // if (FE_METAVERSION < VERSION_94) ... 
    // these clause will be useless and we could remove them 
    public static final int MINIMUM_VERSION_REQUIRED = VERSION_100;
}
