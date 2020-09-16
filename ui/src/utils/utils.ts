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

/**
 * @file test cron
 * @author lpx
 * @since 2020/08/19
 */
function isSuccess(response) {
    if (!response) {
        return false;
    }

    let status = response.status;
    if (status == null) {
        status = response.msg;
    }

    if (isNaN(status)) {
        return status === 'success';
    }
    return status === 0;
}
function getDbName(params) {
    const infoArr = location.pathname.split('-');
    const db_name = infoArr[0].split('/')[3];
    const tbl_name = infoArr[1];
    const res = {};
    res.db_name = db_name;
    res.tbl_name = tbl_name;
    return res;
}
function getTimeNow() {
    let dateNow = new Date();
    let fmt = 'yyyy-MM-dd hh:mm:ss';
    var o = {
        'M+': dateNow.getMonth() + 1,
        'd+': dateNow.getDate(),
        'h+': dateNow.getHours(),
        'm+': dateNow.getMinutes(),
        's+': dateNow.getSeconds(),
        'q+': Math.floor((dateNow.getMonth() + 3) / 3),
        'S': dateNow.getMilliseconds()
    };
    if (/(y+)/.test(fmt)) {
        fmt = fmt.replace(
          RegExp.$1,
          (dateNow.getFullYear() + '').substr(4 - RegExp.$1.length)
        );
    }
    for (var k in o) {
        if (new RegExp('(' + k + ')').test(fmt)) {
            fmt = fmt.replace(
                RegExp.$1,
                RegExp.$1.length === 1
                ? o[k]
                : ('00' + o[k]).substr(('' + o[k]).length)
            );
        }
    }
    return fmt;
}

module.exports = {isSuccess, getDbName, getTimeNow};