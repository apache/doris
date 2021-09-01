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
