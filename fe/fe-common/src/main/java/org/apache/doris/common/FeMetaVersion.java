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
    // add row policy
    public static final int VERSION_109 = 109;
    // For routine load user info
    public static final int VERSION_110 = 110;
    // add catalog PrivTable in Auth to support unified privilege management
    public static final int VERSION_111 = 111;
    // add row policy and add maxColUniqueId for olapTable
    public static final int VERSION_112 = 112;
    // add password options
    public static final int VERSION_113 = 113;
    // add new recover info for recover ddl
    public static final int VERSION_114 = 114;
    // change replica meta to json
    public static final int VERSION_115 = 115;
    // change Auto to rbac
    public static final int VERSION_116 = 116;
    // add user and comment to load job
    public static final int VERSION_117 = 117;
    // change frontend meta to json, add hostname to MasterInfo
    public static final int VERSION_118 = 118;
    // TablePropertyInfo add db id
    public static final int VERSION_119 = 119;
    // For export job
    public static final int VERSION_120 = 120;
    // For BackendHbResponse node type
    public static final int VERSION_121 = 121;
    // For IndexChangeJob
    public static final int VERSION_122 = 122;
    // For AnalysisInfo
    public static final int VERSION_123 = 123;
    // For auto-increment column
    public static final int VERSION_124 = 124;
    // For write/read auto create partition expr
    public static final int VERSION_125 = 125;
    // For write/read function nullable mode info
    public static final int VERSION_126 = 126;
    // For constraints
    public static final int VERSION_127 = 127;
    // For statistics. Update rows, new partition loaded, AnalysisJobInfo and AnalysisTaskInfo
    public static final int VERSION_128 = 128;
    // For table version
    public static final int VERSION_129 = 129;

    public static final int VERSION_130 = 130;

    // for java-udtf add a bool field to write
    public static final int VERSION_131 = 131;

    // For transaction insert
    public static final int VERSION_132 = 132;
    // for expr serde
    public static final int VERSION_133 = 133;
    // For mate gson
    public static final int VERSION_134 = 134;
    // For mate gson
    public static final int VERSION_135 = 135;
    // For mate gson
    public static final int VERSION_136 = 136;
    // For mate gson
    public static final int VERSION_137 = 137;

    public static final int VERSION_138 = 138;

    public static final int VERSION_139 = 139;

    public static final int VERSION_140 = 140;

    // note: when increment meta version, should assign the latest version to VERSION_CURRENT
    public static final int VERSION_CURRENT = VERSION_140;


    // all logs meta version should >= the minimum version, so that we could remove many if clause, for example
    // if (FE_METAVERSION < VERSION_94) ...
    // these clause will be useless and we could remove them
    public static final int MINIMUM_VERSION_REQUIRED = VERSION_100;
}
