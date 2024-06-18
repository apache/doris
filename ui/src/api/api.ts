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

import {API_BASE} from 'Constants';
import request from 'Utils/request';
import {Result} from '@src/interfaces/http.interface';

//login
export function login<T>(data: any): Promise<Result<T>> {
    return request('/rest/v1/login', {
        method: 'POST',
        headers: {Authorization: data.password ? `Basic ${btoa(data.username + ':' + data.password)}` : `Basic ${btoa(data.username + ':')}`},
    });
}

//logout
export function logOut<T>(): Promise<Result<T>> {
    return request(`/rest/v1/logout`, {
        method: 'POST',
    });
}

//home
export function getHardwareInfo<T>(): Promise<Result<T>> {
    return request(`/rest/v1/hardware_info/fe/`, {
        method: 'GET',
    });
}

//system
export function getSystem<T>(data: any): Promise<Result<T>> {
    return request(`/rest/v1/system${data.path}`, {
        method: 'GET',
        ...data
    });
}

//log
export function getLog<T>(data: any): Promise<Result<T>> {
    let localUrl = '/rest/v1/log';
    if (data.add_verbose) {
        localUrl = `/rest/v1/log?add_verbose=${data.add_verbose}`;
    }
    if (data.del_verbose) {
        localUrl = `/rest/v1/log?del_verbose=${data.del_verbose}`;
    }
    // if (data.add_verbose && data.del_verbose) {
    //     localUrl += `/rest/v1/log?add_verbose=${data.add_verbose}&&del_verbose=${data.del_verbose}`;
    // }
    return request(localUrl, {
        method: (data.add_verbose || data.del_verbose) ? 'POST' : 'GET',
        ...data
    });
}

//query_profile
export function queryProfile<T>(data: any): Promise<Result<T>> {
    let LocalUrl = '/rest/v1/query_profile/';
    if (data.path) {
        LocalUrl = `/rest/v1/query_profile/${data.path}`;
    }
    return request(LocalUrl, data);
}

//session
export function getSession<T>(data: any): Promise<Result<T>> {
    return request('/rest/v1/session', data);
}

//config
export function getConfig<T>(data: any): Promise<Result<T>> {
    return request('/rest/v1/config/fe/', data);
}

//query begin
export function getDatabaseList<T>(data?: any): Promise<Result<T>> {
    let reURL = `${API_BASE}default_cluster/databases`;
    if (data) {
        if (data.db_name) {
            reURL += `/${data.db_name}/tables`;
        }
        if (data.db_name && data.tbl_name) {
            reURL += `/${data.tbl_name}/schema`;
        }
    }
    return request(reURL, data);
}

export function doQuery<T>(data: any): Promise<Result<T>> {
    return request(`/api/query/internal/${data.db_name}`, {
        method: 'POST', ...data,
    });
}

export function doUp<T>(data: any): Promise<Result<T>> {
    let localHeader = {
        label: data.label,
        columns: data.columns,
        column_separator: data.column_separator
    }
    if (!localHeader.columns) {
        delete localHeader.columns
    }
    return request(`/api/default_cluster/${data.db_name}/${data.tbl_name}/upload?file_id=${data.file_id}&file_uuid=${data.file_uuid}`, {
        method: 'PUT', headers: localHeader,
    });
}

export function getUploadData<T>(data: any): Promise<Result<T>> {
    let localUrl = `/api/default_cluster/${data.db_name}/${data.tbl_name}/upload`
    if (data.preview) {
        localUrl = `/api/default_cluster/${data.db_name}/${data.tbl_name}/upload?file_id=${data.file_id}&file_uuid=${data.file_uuid}&preview=true`
    }
    return request(localUrl, {
        method: 'GET',
    });
}

export function deleteUploadData<T>(data: any): Promise<Result<T>> {
    return request(`/api/default_cluster/${data.db_name}/${data.tbl_name}/upload?file_id=${data.file_id}&file_uuid=${data.file_uuid}`, {
        method: 'DELETE',
    });
}

//query end
export const AdHocAPI = {
    getDatabaseList,
    doQuery,
    doUp,
    getLog,
    queryProfile,
    logOut,
    getHardwareInfo,
    getUploadData,
    deleteUploadData,
};
