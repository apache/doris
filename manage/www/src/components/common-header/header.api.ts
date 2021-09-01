/** @format */

import { http } from '@src/utils/http';
// import { GetDatabaseInfoByDbIdRequestParams, GetDatabaseRequestParams }   from './header.interface';

function refreshData() {
    return http.post(`/api/meta/sync`, {});
}

export const HeaderAPI = {
    refreshData,
};
