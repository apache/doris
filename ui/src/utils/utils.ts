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

    let {code, msg} = response;

    return code === 0 && msg === 'success';
}

function getDbName(params?: any) {
    const infoArr = location.pathname.split('/');
    const str = infoArr[infoArr.length - 1];
    const res: any = {};
    if (str && str !== 'Playground') {
        const db_name = str.split('-')[0];
        const tbl_name = str.split('-')[1];
        res.db_name = db_name;
        res.tbl_name = tbl_name;
    }
    return res;
}

function getTimeNow() {
    let dateNow = new Date();
    let fmt = 'yyyy-MM-dd hh:mm:ss';
    let o = {
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
    for (let k in o) {
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

function getBasePath() {
    let arr = location.pathname.split('/');
    let res = '';
    if (arr.length > 5) {
        arr = arr.slice(0, 5);
        res = arr.join('/');
    }
    return res;
}

function replaceToTxt(str: string) {
  var regexpNbsp = /&nbsp;/g;
  var reBr = /<\/br>/g;
  const strNoNbsp = str.replace(regexpNbsp, " ");
  const strNoBr = strNoNbsp.replace(reBr, "\r\n");
  return strNoBr;
}

function checkLogin() {
    const username = localStorage.getItem('username');
    if (username) {
        return true;
    }
    return false;
}

export {isSuccess, getDbName, getTimeNow, getBasePath, replaceToTxt, checkLogin};
