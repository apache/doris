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
import { IResult } from 'src/interfaces/http.interface';

function getBENodes(): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/monitor/value/be_list`);
}

function getBE_CPU_IDLE(start?: any, end?: any, data?: any): Promise<IResult<any>> {
    return http.post(`/api/rest/v2/manager/monitor/timeserial/be_cpu_idle?start=${start}&end=${end}`, data);
}

function getBE_Mem(start?: any, end?: any, data?: any): Promise<IResult<any>> {
    return http.post(`/api/rest/v2/manager/monitor/timeserial/be_mem?start=${start}&end=${end}`, data);
}

function getBE_DiskIO(start?: any, end?: any, data?: any): Promise<IResult<any>> {
    return http.post(`/api/rest/v2/manager/monitor/timeserial/be_disk_io?start=${start}&end=${end}`, data);
}

function getBE_base_compaction_score(start?: any, end?: any, data?: any): Promise<IResult<any>> {
    console.log(data);
    return http.post(
        `/api/rest/v2/manager/monitor/timeserial/be_base_compaction_score?start=${start}&end=${end}`,
        data,
    );
}

function getBE_cumu_compaction_score(start?: any, end?: any, data?: any): Promise<IResult<any>> {
    return http.post(
        `/api/rest/v2/manager/monitor/timeserial/be_cumu_compaction_score?start=${start}&end=${end}`,
        data,
    );
}

export const MonitorAPI = {
    getBENodes,
    getBE_CPU_IDLE,
    getBE_Mem,
    getBE_DiskIO,
    getBE_base_compaction_score,
    getBE_cumu_compaction_score,
};
