/** @format */

import { http } from '@src/utils/http';
import {
    GetDatabaseInfoByDbIdRequestParams,
    GetDatabaseRequestParams,
    GetTablesByDbIdRequestParams,
    ResNode,
    NewDatabaseRequestParams,
} from './tree.interface';
import { IResult } from 'src/interfaces/http.interface';
import { DEFAULT_NAMESPACE_ID } from '@src/config';

function getDatabaseList(data: GetDatabaseRequestParams): Promise<IResult<[ResNode]>> {
    return http.get(`/api/meta/nsId/${data.nsId}/databases`);
}
function getDatabaseInfo(data: GetDatabaseInfoByDbIdRequestParams) {
    return http.get(`/api/meta/dbId/${data.dbId}/info`);
}
function getTables(data: GetTablesByDbIdRequestParams): Promise<IResult<[ResNode]>> {
    return http.get(`/api/meta/dbId/${data.dbId}/tables`);
}
function newDatabase(data: NewDatabaseRequestParams) {
    return http.post(`/api/build/nsId/${DEFAULT_NAMESPACE_ID}/database`, data);
}
export const TreeAPI = {
    getDatabaseList,
    getDatabaseInfo,
    getTables,
    newDatabase,
};
