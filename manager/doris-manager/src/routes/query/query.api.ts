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

/** @format */

import { http } from '@src/utils/http';
import { IResult } from '@src/interfaces/http.interface';

// 查询主页
export function getQueryInfo(data?: any): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/query/query_info`, data);
}

//  sql语句
export function getSQLText(data?: any): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/query/sql/${data.queryId}`, {}, { responseType: 'json' });
}

// profile text
export function getProfileText(data?: any): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/query/profile/text/${data.queryId}`);
}

// profile fragments
export function getProfileFragments(data?: any): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/query/profile/fragments/${data.queryId}`);
}

// profile graph
export function getProfileGraph(data?: any): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/query/profile/graph/${data.queryId}`, {
        fragment_id: data.fragmentId,
        instance_id: data.instanceId,
    });
}
