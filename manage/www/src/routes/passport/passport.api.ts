/** @format */

import { http } from '@src/utils/http';
function SessionLogin(_value: any) {
    return http.post(`/api/session/`, _value);
}
export const PassportAPI = {
    SessionLogin,
};
