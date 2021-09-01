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
