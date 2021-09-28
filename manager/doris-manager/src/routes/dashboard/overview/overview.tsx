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
import styles from './overview.less';
import { useTranslation } from 'react-i18next';
import { DashboardAPI } from '../dashboard.api';
import { DashboardItem } from '../components/dashboard-item';
import { HddOutlined, LaptopOutlined, PieChartOutlined, TableOutlined } from '@ant-design/icons';
import { message } from 'antd';
import { MetaInfoResponse } from '../dashboard.interface';

export function Component(props: any) {
    const { t } = useTranslation();
    const [metaInfo, setMetaInfo] = useState<MetaInfoResponse>({
        beCount: 0,
        dbCount: 0,
        diskOccupancy: 0,
        feCount: 0,
        remainDisk: 0,
        tblCount: 0,
    });
    useEffect(() => {
        getOverviewInfo();
    }, []);
    async function getOverviewInfo() {
        const res = await DashboardAPI.getMetaInfo();
        const { msg, data, code } = res;
        if (code === 0) {
            if (res.data) {
                res.data.remainDisk = res.data.remainDisk / 1024;
                setMetaInfo(res.data);
            }
        } else {
            message.error(msg);
        }
    }
    return (
        <div styleName="overview-container">
            <DashboardItem title={t`DatabseNum`} icon={<HddOutlined />} des={metaInfo.dbCount} />
            <DashboardItem title={t`DataTableNum`} icon={<TableOutlined />} des={metaInfo.tblCount} />
            <DashboardItem
                title={t`DiskUsage`}
                icon={<PieChartOutlined />}
                des={
                    metaInfo.diskOccupancy == 0 ? metaInfo.diskOccupancy + '%' : metaInfo.diskOccupancy.toFixed(2) + '%'
                }
            />
            <DashboardItem title={t`RemainingDiskSpace`} icon={<LaptopOutlined />} des={metaInfo.remainDisk.toFixed(2) + 'TB'} />
        </div>
    );
}

export const Overview = CSSModules(styles)(Component);
