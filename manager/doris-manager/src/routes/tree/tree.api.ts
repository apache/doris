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
import {
    GetDatabaseInfoByDbIdRequestParams,
    GetDatabaseRequestParams,
    GetTablesByDbIdRequestParams,
    ResNode,
    NewDatabaseRequestParams,
} from './tree.interface';
import { IResult } from 'src/interfaces/http.interface';

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
    return http.post(`/api/build/nsId/0/database`, data);
}
export const TreeAPI = {
    getDatabaseList,
    getDatabaseInfo,
    getTables,
    newDatabase,
};
