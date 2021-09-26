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

import { http } from '@src/utils/http';

function getConfigSelect() {
    return http.get(`/api/rest/v2/manager/node/configuration_name`);
}
function getNodeSelect() {
    return http.get(`/api/rest/v2/manager/node/node_list`);
}
function getConfigurationsInfo(data: any, type: string) {
    return http.post(`/api/rest/v2/manager/node/configuration_info?type=${type}`, data);
}
function setConfig(data: any) {
    return http.post(`/api/rest/v2/manager/node/set_config/fe`, data);
}
export const FeConfigAPI = {
    getConfigSelect,
    getNodeSelect,
    getConfigurationsInfo,
    setConfig,
};
