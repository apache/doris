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
import React, { useState } from 'react';
import styles from './dashboard.less';
import { CommonHeader } from '@src/components/common-header/header';
import { ConnectInfo } from './connect-info/connect-info';
import { DashboardTabEnum } from './dashboard.data';
import { HomeOutlined } from '@ant-design/icons';
import { Link, match, Redirect, Route, Switch, useHistory, useLocation } from 'react-router-dom';
import { Overview } from './overview/overview';
import { Tabs } from 'antd';
import { useTranslation } from 'react-i18next';

const ICON_HOME = <HomeOutlined />;
const { TabPane } = Tabs;

function Component(props: any) {
    const { match } = props;
    const { t } = useTranslation();
    const [refreshToken, setRefreshToken] = useState(new Date().getTime());
    const history = useHistory();
    const location = useLocation();
    const [activeKey, setTabsActiveKey] = useState<DashboardTabEnum>(DashboardTabEnum.Overview);
    React.useEffect( ()=>{
        if(location.pathname.includes(DashboardTabEnum.ConnectInfo)) {
            setTabsActiveKey(DashboardTabEnum.ConnectInfo);
        } else {
            setTabsActiveKey(DashboardTabEnum.Overview);
        }
    }, [])
    return (
        <div styleName="home-main">
            <CommonHeader
                title={t`dataWarehouse`}
                icon={ICON_HOME}
                callback={() => setRefreshToken(new Date().getTime())}
            ></CommonHeader>
            <div styleName="home-content">
                <Tabs activeKey={activeKey} onChange={(key: any) => {
                    setTabsActiveKey(key);
                    if (key === DashboardTabEnum.Overview) {
                        history.push(`${match.path}/overview`);
                    } else {
                        history.push(`${match.path}/connect-info`);
                    }
                }}>
                    <TabPane tab={t`ClusterIinformationOverview`} key={DashboardTabEnum.Overview}></TabPane>
                    <TabPane tab={t`ConnectionInformation`} key={DashboardTabEnum.ConnectInfo}></TabPane>
                </Tabs>
            </div>
            <Switch>
                <Route path={`${match.path}/overview`} component={Overview} />
                <Route path={`${match.path}/connect-info`} component={ConnectInfo} />
                <Redirect to={`${match.path}/overview`} />
            </Switch>
        </div>
    );
}
export const Dashboard = CSSModules(styles)(Component);
