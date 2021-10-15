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

import React, { lazy } from 'react';
import styles from './container.less';
import { Dashboard } from './dashboard/dashboard';
import { Layout } from 'antd';
import { Redirect, Route, Router, Switch } from 'react-router-dom';
import { Sidebar } from '@src/components/sidebar/sidebar';
import { Header } from '@src/components/header/header';
const Workspace = lazy(() => import('./workspace/workspace'));
const Login = lazy(() => import('./passport/index'));

const Content = lazy(() => import('./content/index'));

const Cluster = lazy(() => import('./cluster/index'));

const NodeList = lazy(() => import('./node/list'));
const FEConfiguration = lazy(() => import('./node/list/fe-configuration/index'));
const BEConfiguration = lazy(() => import('./node/list/be-configuration/index'));

const Configuration = lazy(() => import('./node/list/configuration'));
const NodeDash = lazy(() => import('./node/dashboard'));

const Query = lazy(() => import('./query/index'));
const QueryDetails = lazy(() => import('./query/query-details/index'));
const UserSetting = lazy(() => import('./user-setting/index'));
class Container extends React.Component<any, {}> {
    constructor(props: any) {
        super(props);
    }

    render() {
        return (
            <Router history={this.props.history}>
                <Layout style={{height: '100vh'}}>
                    <Layout>
                        <Sidebar width={200} className="DAE-manager-side" />
                        <div className={styles['container-content']}>
                            <Header>
                                
                            </Header>
                            <Switch>
                                <Route path="/dashboard" component={Dashboard} />
                                {/* 数据 */}
                                <Route path="/:nsId/content" component={Content}/>
                                
                                {/* 集群 */}
                                <Route path="/:nsId/dash" component={Cluster} />
                                {/* 节点 */}
                                <Route path="/:nsId/list" component={NodeList} />
                                <Route path="/:nsId/fe-configuration" component={FEConfiguration} />
                                <Route path="/:nsId/be-configuration" component={BEConfiguration} />
                                <Route path="/:nsId/configuration" component={Configuration} />
                                <Route path="/:nsId/node-dash" component={NodeDash} />
                                {/* 查询 */}
                                <Route path="/:nsId/query" component={Query} />
                                <Route path="/:nsId/details/:queryId" component={QueryDetails} />
                                { /*账户设置*/}
                                <Route path="/:nsId/user-setting" component={UserSetting} />
                                <Redirect to="/dashboard" />
                              
                            </Switch>
                        </div>
                    </Layout>
                </Layout>
            </Router>
        );
    }
}

export default Container;
