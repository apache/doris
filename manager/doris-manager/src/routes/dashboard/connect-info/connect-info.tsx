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

import CSSModules from 'react-css-modules';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styles from './connect-info.less';
import { Card, message } from 'antd';
import { DashboardAPI } from '../dashboard.api';

export function Component(props: any) {
    const { t } = useTranslation();
    const [spaceList, setSpaceList] = useState<any>({});
    const [https, setHttps] = useState<any>([]);
    const [mysqls, setMysqls] = useState<any>([]);
    useEffect(() => {
        getOverviewInfo();
    }, []);
    async function getOverviewInfo() {
        DashboardAPI.getSpaceList().then(res1 => {
            const { msg, data, code } = res1;
            let http = '';
            let mysql = '';
            if (code === 0) {
                if (res1.data) {
                    setSpaceList(res1.data);
                    for (let i = 0; i < res1.data.http.length; i++) {
                        http += res1.data.http[i] + '; ';
                    }
                    for (let i = 0; i < res1.data.mysql.length; i++) {
                        mysql += res1.data.mysql[i] + '; ';
                    }
                    setHttps(http);
                    setMysqls(mysql);
                }
            } else if (code === 404) {
                message.error(t`updateDoris`);
                window.location.href = `${window.location.origin}`;
            } else {
                message.error(msg);
            }
        });
    }
    return (
        <div styleName="connect-info-container">
            <Card style={{ width: '100%' }}>
                <p>{t`httpInfo`} {https}</p>
                <p>{t`JDBCInfo`} {mysqls}</p>
            </Card>
        </div>
    );
}

export const ConnectInfo = CSSModules(styles)(Component);
