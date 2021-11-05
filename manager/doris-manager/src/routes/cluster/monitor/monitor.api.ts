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

function getStatisticInfo(data?: any): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/monitor/value/node_num`);
}
function getDisksCapacity(data?: any): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/monitor/value/disks_capacity`);
}

function getQPS(data?: any): Promise<IResult<any>> {
    return http.post(`/api/rest/v2/manager/monitor/timeserial/${data.type}?start=${data.start}&end=${data.end}`, data);
}
function getQueryLatency(data?: any): Promise<IResult<any>> {
    return http.post(`/api/rest/v2/manager/monitor/timeserial/query_latency?start=${data.start}&end=${data.end}`, data);
}

function getErrorRate(data?: any): Promise<IResult<any>> {
    return http.post(
        `/api/rest/v2/manager/monitor/timeserial/query_err_rate?start=${data.start}&end=${data.end}`,
        data,
    );
}
function getConnectionTotal(data?: any): Promise<IResult<any>> {
    return http.post(`/api/rest/v2/manager/monitor/timeserial/conn_total?start=${data.start}&end=${data.end}`, data);
}
function getScheduled(data?: any): Promise<IResult<any>> {
    return http.post(
        `/api/rest/v2/manager/monitor/timeserial/scheduled_tablet_num?start=${data.start}&end=${data.end}`,
        data,
    );
}

function getStatistic(): Promise<IResult<any>> {
    return http.get(`/api/rest/v2/manager/monitor/value/statistic`);
}
function getTxnStatus(data?: any): Promise<IResult<any>> {
    return http.post(`/api/rest/v2/manager/monitor/timeserial/txn_status?start=${data.start}&end=${data.end}`, data);
}

export const MonitorAPI = {
    getStatisticInfo,
    getDisksCapacity,
    getQPS,
    getQueryLatency,
    getErrorRate,
    getConnectionTotal,
    getTxnStatus,
    getScheduled,
    getStatistic,
};
