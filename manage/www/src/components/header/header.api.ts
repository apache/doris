/* eslint-disable prettier/prettier */
/** @format */

import { http } from '@src/utils/http';
function getCurrentUser() {
    return http.get(`/api/user/current`);
}

function getSpaceName(data: any) {
    return http.get(`/api/space/${data}`);
}

function signOut() {
    return http.delete(`/api/session/`);
}
export const LayoutAPI = {
    getCurrentUser,
    getSpaceName,
    signOut,
};
