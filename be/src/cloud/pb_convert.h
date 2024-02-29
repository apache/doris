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

#include <gen_cpp/olap_file.pb.h>

namespace doris::cloud {

// RowsetMetaPB <=> RowsetMetaCloudPB
RowsetMetaCloudPB doris_rowset_meta_to_cloud(const RowsetMetaPB&);
RowsetMetaCloudPB doris_rowset_meta_to_cloud(RowsetMetaPB&&);
void doris_rowset_meta_to_cloud(RowsetMetaCloudPB* out, const RowsetMetaPB& in);
void doris_rowset_meta_to_cloud(RowsetMetaCloudPB* out, RowsetMetaPB&& in);
RowsetMetaPB cloud_rowset_meta_to_doris(const RowsetMetaCloudPB&);
RowsetMetaPB cloud_rowset_meta_to_doris(RowsetMetaCloudPB&&);
void cloud_rowset_meta_to_doris(RowsetMetaPB* out, const RowsetMetaCloudPB& in);
void cloud_rowset_meta_to_doris(RowsetMetaPB* out, RowsetMetaCloudPB&& in);

// TabletSchemaPB <=> TabletSchemaCloudPB
TabletSchemaCloudPB doris_tablet_schema_to_cloud(const TabletSchemaPB&);
TabletSchemaCloudPB doris_tablet_schema_to_cloud(TabletSchemaPB&&);
void doris_tablet_schema_to_cloud(TabletSchemaCloudPB* out, const TabletSchemaPB& in);
void doris_tablet_schema_to_cloud(TabletSchemaCloudPB* out, TabletSchemaPB&& in);
TabletSchemaPB cloud_tablet_schema_to_doris(const TabletSchemaCloudPB&);
TabletSchemaPB cloud_tablet_schema_to_doris(TabletSchemaCloudPB&&);
void cloud_tablet_schema_to_doris(TabletSchemaPB* out, const TabletSchemaCloudPB& in);
void cloud_tablet_schema_to_doris(TabletSchemaPB* out, TabletSchemaCloudPB&& in);

// TabletMetaPB <=> TabletMetaCloudPB
TabletMetaCloudPB doris_tablet_meta_to_cloud(const TabletMetaPB&);
TabletMetaCloudPB doris_tablet_meta_to_cloud(TabletMetaPB&&);
void doris_tablet_meta_to_cloud(TabletMetaCloudPB* out, const TabletMetaPB& in);
void doris_tablet_meta_to_cloud(TabletMetaCloudPB* out, TabletMetaPB&& in);
TabletMetaPB cloud_tablet_meta_to_doris(const TabletMetaCloudPB&);
TabletMetaPB cloud_tablet_meta_to_doris(TabletMetaCloudPB&&);
void cloud_tablet_meta_to_doris(TabletMetaPB* out, const TabletMetaCloudPB& in);
void cloud_tablet_meta_to_doris(TabletMetaPB* out, TabletMetaCloudPB&& in);

} // namespace doris::cloud
