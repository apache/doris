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

import moment from 'moment';
import { message } from 'antd';
import { TableAPI } from '@src/routes/table-content/table.api';

export const getDefaultPageSize = () => JSON.parse(localStorage.getItem('pageSize') || '50');
export const deepClone = (data: any) => JSON.parse(JSON.stringify(data));

export function getShowTime(time: string) {
    if (time) {
        const res = moment(time, moment.ISO_8601).local().format('YYYY-MM-DD HH:mm:ss');
        return res;
    }
}
function getIdFromUrl(href: any) {
    if (href.indexOf('meta/table/') >= 0) {
        return href.split('meta/table/')[1].split('/')[0];
    } else if (href.indexOf('local-import/') >= 0) {
        return href.split('local-import/')[1];
    } else if (href.indexOf('system-import/') >= 0) {
        return href.split('system-import/')[1];
    } else {
        return '';
    }
}

export function formatBytes(bytes: number, decimals = 2, showSize = true) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024,
        dm = decimals,
        sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
        i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + (showSize ? ' ' + sizes[i] : '');
}

export function isTableIdSame() {
    const tableId = getIdFromUrl(window.location.href);
    const local_table_id = localStorage.getItem('table_id');
    if (tableId && local_table_id !== tableId) {
        TableAPI.getTableInfo({ tableId: tableId }).then(res => {
            const { msg, code, data } = res;
            if (code === 0) {
                localStorage.setItem('database_id', data.dbId);
                localStorage.setItem('database_name', data.dbName);
                localStorage.setItem('table_id', tableId);
                localStorage.setItem('table_name', data.name);
                window.location.reload();
            } else {
                message.error(msg);
            }
        });
    }
}