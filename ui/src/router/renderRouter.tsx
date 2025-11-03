/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React from 'react';
import { Route, Redirect, Switch } from 'react-router-dom';
import { checkLogin, getBasePath } from 'Src/utils/utils';

let isLogin = checkLogin();
const renderRoutes = (routes, authPath = '/login') => {
    let basepath = getBasePath();
    if (routes) {
        return (
            <Switch>
                {routes.map((route, i) => (
                    <Route
                        key={route.key || i}
                        path={route.path}
                        exact={route.exact}
                        strict={route.strict}
                        render={(props) => {
                            if (props.location.pathname === basepath + '/') {
                                return <Redirect to={'/home'} />;
                            }
                            if (isLogin) {
                                return route.render ? (
                                    route.render({ ...props, route: route })
                                ) : (
                                    <route.component {...props} route={route} />
                                );
                            } else {
                                isLogin = '1';
                                return <Redirect to={authPath} />;
                            }
                        }}
                    />
                ))}
            </Switch>
        );
    }
    return null;
};

export default renderRoutes;
