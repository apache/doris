import React, { lazy } from 'react';
import SuperAdminContainer from './routes/super-admin-container';
import { auth } from './utils/auth';
import { Loading } from './components/loading';
import { NotFound } from './components/not-found';
import { Suspense } from 'react';
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

import { Route, Switch, Redirect, BrowserRouter as Router } from 'react-router-dom';
import { Container } from './routes/container';
const Login = lazy(() => import('../src/routes/passport/index'));

const user = JSON.parse(window.localStorage.getItem('user') as string);
// 对状态属性进行监听
const routes = (
    <Suspense fallback={<Loading />}>
        <Router>
            <Switch>
                <Route
                    path="/"
                    render={props =>
                        auth.checkLogin() ? (
                            user.is_super_admin ? (
                                <SuperAdminContainer {...props} />
                            ) : (
                                <Container {...props} />
                            )
                        ) : props.history.location.pathname === '/login' ? (
                            <Login></Login>
                        ) : (
                            <Redirect to="/login" />
                        )
                    }
                />
                <Route path="/login" render={props => (auth.checkLogin() ? <Redirect to="/" /> : <></>)} />
                <Route component={NotFound} />
            </Switch>
        </Router>
    </Suspense>
);

export default routes;
