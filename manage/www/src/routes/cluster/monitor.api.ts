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
