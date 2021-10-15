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
import { MetaInfoResponse } from './space.interface';
import { IResult } from 'src/interfaces/http.interface';
function spaceCreate(params: any): Promise<IResult<MetaInfoResponse>> {
    return http.post(`/api/space/create`, params);
}
function spaceList(): Promise<IResult<MetaInfoResponse>> {
    return http.get(`/api/space/all`);
}
function spaceCheck(name: string): Promise<IResult<MetaInfoResponse>> {
    return http.get(`/api/space/name/${name}/check`);
}
function spaceValidate(data: any): Promise<IResult<MetaInfoResponse>> {
    return http.post(`/api/space/validate`, data);
}
function spaceDel(): Promise<IResult<MetaInfoResponse>> {
    return http.post(`/api/space/:spaceId`);
}
function spaceGet(spaceId: string): Promise<IResult<MetaInfoResponse>> {
    return http.get(`/api/space/${spaceId}`);
}
function spaceUpdate(): Promise<IResult<MetaInfoResponse>> {
    return http.put(`/api/space/:spaceId/update`);
}
export const SpaceAPI = {
    spaceCreate,
    spaceList,
    spaceCheck,
    spaceValidate,
    spaceDel,
    spaceGet,
    spaceUpdate,
};
