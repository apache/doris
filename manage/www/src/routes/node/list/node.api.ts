import { http } from '@src/utils/http';
function getFrontends() {
    return http.get(`/api/rest/v2/manager/node/frontends`);
}
function getBrokers() {
    return http.get(`/api/rest/v2/manager/node/brokers`);
}
function getBackends() {
    return http.get(`/api/rest/v2/manager/node/backends`);
}

export const NodeAPI = {
    getFrontends,
    getBrokers,
    getBackends,
};
