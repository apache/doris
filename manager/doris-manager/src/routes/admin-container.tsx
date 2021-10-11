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
import { Redirect, Route, Router, Switch } from 'react-router-dom';
import { Header } from '@src/components/header/header';
import { Layout } from 'antd';

const SpaceList = lazy(() => import('./space/list'));
const SpaceNew = lazy(() => import('./space/index'));
const SpaceCheck = lazy(() => import('./space/space-check'));

class AdminContainer extends React.Component<any, {}> {
    constructor(props: any) {
        super(props);
    }

    render() {
        return (
            <Router history={this.props.history}>
              <Layout style={{height: '100vh'}}>
                <Header></Header>
                <Switch>
                    {/* 空间 */}
                    <Route path="/space/list" component={SpaceList} />
                    <Route path="/space/check/:spaceId" component={SpaceCheck} />
                    <Route path="/space/new" component={SpaceNew} />
                    <Redirect to="/space/list" />
                </Switch>
              </Layout>
                
            </Router>
        );
    }
}

export default AdminContainer;
