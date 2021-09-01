/** @format */

import { http } from '@src/utils/http';
import { GetTableListRequestParams, TableListResponse } from './tabs.interface';
import { IResult } from 'src/interfaces/http.interface';
function getTableList(data: GetTableListRequestParams) {
    return http.get(`/api/import/tableId/${data.tableId}/task/list/${data.page}/${data.pageSize}`);
}

export const TableAPI = {
    getTableList,
};
