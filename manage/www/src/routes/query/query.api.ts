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
