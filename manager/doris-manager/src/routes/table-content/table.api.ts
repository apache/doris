// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
