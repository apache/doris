import { http } from '@src/utils/http';
import { IResult } from '@src/interfaces/http.interface';
import { MetaInfoResponse } from './dashboard.interface';

function getMetaInfo(): Promise<IResult<MetaInfoResponse>> {
    return http.get(`/api/cluster/overview`);
}
function getSpaceList() {
    return http.get(`/api/rest/v2/manager/cluster/cluster_info/conn_info`);
}
function getCurrentUser() {
    return http.get(`/api/user/current`);
}
export const DashboardAPI = {
    getMetaInfo,
    getSpaceList,
    getCurrentUser,
};
