import { http } from "@src/utils/http";

function getUserInfo() {
    return http.get(`/api/user/current`);
}

export const CommonAPI = {
    getUserInfo
}