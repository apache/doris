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

import asyncComponent from '../utils/lazy';
const Playground = asyncComponent(() => import('../pages/playground'));
const QueryRouter = asyncComponent(() => import('../pages/playground/router'));
const Login = asyncComponent(() => import('../pages/login'));
const Layout = asyncComponent(() => import('../pages/layout'));
const Home = asyncComponent(() => import('../pages/home'));
const System = asyncComponent(() => import('../pages/system'));
const Backend = asyncComponent(() => import('../pages/backend'));
const Logs = asyncComponent(() => import('../pages/logs'));
const QueryProfile = asyncComponent(() => import('../pages/query-profile'));
const Session = asyncComponent(() => import('../pages/session'));
const Configuration = asyncComponent(() => import('../pages/configuration'));
// const Ha = asyncComponent(() => import('../pages/ha'));
// const Help = asyncComponent(() => import('../pages/help'));
const Page404 = asyncComponent(() => import('../pages/404'));
const DataImport = asyncComponent(() => import('../pages/playground/data-import'));
export default {
    routes: [
        {
            path: '/login',
            component: Login,
            title: 'Login',
        },
        {
            path: '/',
            component: Layout,
            routes: [
                {
                    path: '/home',
                    component: Home,
                    title: 'Home',
                },
                {
                    path: '/Playground',
                    component: QueryRouter,
                    title: 'Playground',
                    routes: [
                        {
                            path: '/Playground/import',
                            component: DataImport,
                        },
                        {
                            path: '/Playground',
                            component: Playground,
                        },
                    ],
                },
                {
                    path: '/System',
                    component: System,
                    title: 'System',
                    search: '?path=/',
                },
                // {
                //     path: '/menu/backend',
                //     component: Backend,
                //     title: 'backend',
                // },
                {
                    path: '/Log',
                    component: Logs,
                    title: 'Log',
                },
                {
                    path: '/QueryProfile',
                    component: QueryProfile,
                    title: 'QueryProfile',
                },
                {
                    path: '/Session',
                    component: Session,
                    title: 'Session',
                },
                {
                    path: '/Configuration',
                    component: Configuration,
                    title: 'Configuration',
                },
                {
                    path: '*',
                    component: Page404,
                },
                // {
                //     path: '/ha',
                //     component: Ha,
                //     title: 'ha',
                // },
                // {
                //     path: '/help',
                //     component: Help,
                //     title: 'help',
                // },
            ],
        },
        {
            path: '*',
            component: Page404,
        },
        {
            path: '/',
            redirect: '/home',
            component: Layout,
        },
        
    ],
};