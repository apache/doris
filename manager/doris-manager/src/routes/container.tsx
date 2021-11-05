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

import React, { lazy, useEffect, useState } from 'react';
import styles from './container.less';
import { Dashboard } from './dashboard/dashboard';
import { Layout } from 'antd';
import { Redirect, Route, Router, Switch } from 'react-router-dom';
import { Sidebar } from '@src/components/sidebar/sidebar';
import { Header } from '@src/components/header/header';
import UserSetting from './user-setting';
import QueryDetails from './query/query-details';
import Query from './query';
import { NodeDashboard } from './node/dashboard';
import Configuration from './node/list/configuration';
import { Cluster } from './cluster/cluster';
import { CommonAPI } from '@src/common/common.api';
import { UserInfoContext } from '@src/common/common.context';
import { UserInfo } from '@src/common/common.interface';
import { Meta } from './meta/meta';

const NodeList = lazy(() => import('./node/list'));
const FEConfiguration = lazy(() => import('./node/list/fe-configuration/index'));
const BEConfiguration = lazy(() => import('./node/list/be-configuration/index'));

export function Container(props: any) {

    const [userInfo, setUserInfo] = useState<UserInfo | null>(null);
    useEffect(() => {
        CommonAPI.getUserInfo().then((res) => {
            if (res.code === 0) {
                setUserInfo(res.data);
            }
        });
    }, []);

    return (
        <Router history={props.history}>
            <UserInfoContext.Provider value={userInfo}>
            <Layout style={{height: '100vh'}}>
                <Layout>
                    <Sidebar width={200} className="DAE-manager-side" />
                    <div className={styles['container-content']}>
                        <Header>
                            
                        </Header>
                        <Switch>
                            <Route path="/dashboard" component={Dashboard} />
                            {/* 数据 */}
                            <Route path="/meta" component={Meta}/>
                            
                            {/* 集群 */}
                            <Route path="/cluster" component={Cluster} />
                            {/* 节点 */}
                            <Route path="/list" component={NodeList} />
                            <Route path="/configuration/fe" component={FEConfiguration} />
                            <Route path="/configuration/be" component={BEConfiguration} />
                            <Route path="/configuration" component={Configuration} />
                            <Route path="/node-dashboard" component={NodeDashboard} />
                            {/* 查询 */}
                            <Route path="/query" component={Query} />
                            <Route path="/details/:queryId" component={QueryDetails} />
                            { /*账户设置*/}
                            <Route path="/user-setting" component={UserSetting} />
                            <Redirect to="/dashboard" />
                            
                        </Switch>
                    </div>
                </Layout>
            </Layout>
            </UserInfoContext.Provider>
        </Router>
    );
}

