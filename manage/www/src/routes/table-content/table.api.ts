/** @format */

import { http } from '@src/utils/http';
import { GetTableInfoRequestParams, TableInfoResponse, sqlRequestParams } from './table.interface';
import { IResult } from 'src/interfaces/http.interface';
function getTableInfo(data: GetTableInfoRequestParams): Promise<IResult<TableInfoResponse>> {
    return http.get(`/api/meta/tableId/${data.tableId}/info`);
}
function sendSql(data: sqlRequestParams): Promise<IResult<any>> {
    return http.post(`/api/query/nsId/${data.nsId}/dbId/${data.dbId}`, data.data);
}
export const TableAPI = {
    getTableInfo,
    sendSql,
};
