/** @format */

import { http } from '@src/utils/http';
import { GetDatabaseInfoRequestParams, DatabaseInfoResponse } from './database.interface';
import { IResult } from '@src/interfaces/http.interface';
function getDatabaseInfo(params: GetDatabaseInfoRequestParams): Promise<IResult<DatabaseInfoResponse>> {
    return http.get(`/api/meta/dbId/${params.dbId}/info`);
}

export const DatabaseAPI = {
    getDatabaseInfo,
};
