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

package org.apache.doris.datasource.property.constants;

import java.util.Collections;
import java.util.List;

public class HMSProperties {
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";
    public static final String DLF_TYPE = "dlf";
    public static final String GLUE_TYPE = "glue";
    public static final String HIVE_VERSION = "hive.version";
    // required
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final List<String> REQUIRED_FIELDS = Collections.singletonList(HMSProperties.HIVE_METASTORE_URIS);
    public static final String  ENABLE_HMS_EVENTS_INCREMENTAL_SYNC = "hive.enable_hms_events_incremental_sync";
    public static final String  HMS_EVENTIS_BATCH_SIZE_PER_RPC = "hive.hms_events_batch_size_per_rpc";
}
