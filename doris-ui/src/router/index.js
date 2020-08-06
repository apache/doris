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


import Vue from 'vue';
import Router from 'vue-router';
import login from '@/views/login';
import index from '@/views/index';

/**
 * System
 */
//frontend
import Frontends from '@/views/system/Frontends'
//brokers
import Brokers from '@/views/system/Brokers'
//auth
import Auth from '@/views/system/Auth'
import AuthInfo from '@/views/system/AuthInfo'
//routine load
import Routineloads from '@/views/system/Routineloads'
import Jobs from '@/views/job/Jobs'
import Resources from '@/views/system/Resources'

import Monitor from '@/views/system/Monitor'
import MonitorInfo from '@/views/system/MonitorInfo'

import Transactions from '@/views/trans/Transactions'
import Transactions_list from '@/views/trans/Transactions_list'
import Transactions_running_list from '@/views/trans/Transactions_running_list'
import Transactions_finished_list from '@/views/trans/Transactions_finished_list'

import ColocationGroup from '@/views/system/colocation_group'
import Backends from '@/views/system/Backends'
import BackendsInfo from '@/views/system/BackendsInfo'
import Balance from '@/views/system/cluster_balance'
import BalanceInfo from '@/views/system/BalanceInfo'
import Working_slots from '@/views/system/working_slots'
import sched_stat from '@/views/system/sched_stat'
import Priority_repair from '@/views/system/Priority_repair'
import Pending_tablets from '@/views/system/Pending_tablets'
import running_tablets from '@/views/system/running_tablets'
import history_tablets from '@/views/system/history_tablets'
import Current_queries from '@/views/system/current_queries'
import Current_queries_info from '@/views/system/current_queries_info'
import tasks from '@/views/system/tasks'
import statistic from '@/views/system/statistic'
import statistic_info from '@/views/system/statistic_info'

import dbs from '@/views/dbs/dbs'
import dbs_info from '@/views/dbs/dbs_info'
import table_info from '@/views/dbs/table_info'
import partitions from '@/views/dbs/partitions'
import partitions_index from '@/views/dbs/partitions_index'
import partitions_tablet from '@/views/dbs/partitions_tablet'
import partitions_tablet_info from '@/views/dbs/partitions_tablet_info'
import temp_partitions from '@/views/dbs/temp_partitions'
import index_schema from '@/views/dbs/index_schema'
import index_schema_struct from '@/views/dbs/index_schema_struct'
import current_backend_instances from '@/views/system/current_backend_instances'


import Jobs_table_load from '@/views/job/Jobs_table_load'
import Jobs_table_delete from '@/views/job/Jobs_table_delete'
import Jobs_table_rollup from '@/views/job/Jobs_table_rollup'
import Jobs_table_schema_change from '@/views/job/Jobs_table_schema_change'
import Jobs_table_export from '@/views/job/Jobs_table_export'
import Jobs_table from '@/views/job/Jobs_table'


import Query from '@/views/query/Query'
import QueryProfile from '@/views/query/QueryProfile'
import Session from '@/views/session/Session'
import Variable from '@/views/variable/Variable'
import SystemInfo from '@/views/home/home'
import SystemLog from '@/views/syslog/SystemLog'
import HA from '@/views/ha/ha'
import help from '@/views/help/help'

// 启用路由
Vue.use(Router);

// 导出路由 
export default new Router({
    routes: [{
        path: '/',
        name: '',
        component: login,
        hidden: true,
        meta: {
            requireAuth: false
        }
    }, {
        path: '/login',
        name: '登录',
        component: login,
        hidden: true,
        meta: {
            requireAuth: false
        }
    }, {
        path: '/index',
        name: '首页',
        component: index,
        iconCls: 'el-icon-tickets',
        children: [{
            path: '/home/home',
            name: '首页',
            component: SystemInfo,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Frontends',
            name: 'Frontends',
            component: Frontends,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Brokers',
            name: 'Brokers',
            component: Brokers,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Auth',
            name: 'Auth',
            component: Auth,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/AuthInfo/:id',
            name: 'AuthInfo',
            component: AuthInfo,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Routineloads',
            name: 'Routineloads',
            component: Routineloads,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/job/Jobs',
            name: 'Jobs',
            component: Jobs,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/job/Jobs_table/:id',
            name: 'Jobs_table',
            component: Jobs_table,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/job/Jobs_table_load/:id',
            name: 'Jobs_table_load',
            component: Jobs_table_load,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/job/Jobs_table_delete/:id',
            name: 'Jobs_table_delete',
            component: Jobs_table_delete,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/job/Jobs_table_rollup/:id',
            name: 'Jobs_table_rollup',
            component: Jobs_table_rollup,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/job/Jobs_table_schema_change/:id',
            name: 'Jobs_table_schema_change',
            component: Jobs_table_schema_change,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/job/Jobs_table_export/:id',
            name: 'Jobs_table_export',
            component: Jobs_table_export,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Resources',
            name: 'Resources',
            component: Resources,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Monitor',
            name: 'Monitor',
            component: Monitor,
            meta: {
                requireAuth: true
            }
        },{
            path: '/system/MonitorInfo/:id',
            name: 'MonitorInfo',
            component: MonitorInfo,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/session/Session',
            name: 'Session',
            component: Session,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/trans/Transactions',
            name: 'Transactions',
            component: Transactions,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/trans/Transactions_list/:id',
            name: 'Transactions_list',
            component: Transactions_list,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/trans/Transactions_finished_list/:id',
            name: 'Transactions_finished_list',
            component: Transactions_finished_list,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/trans/Transactions_running_list/:id',
            name: 'Transactions_running_list',
            component: Transactions_running_list,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/colocation_group',
            name: 'ColocationGroup',
            component: ColocationGroup,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Backends',
            name: 'Backends',
            component: Backends,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/BackendsInfo/:id',
            name: 'BackendsInfo',
            component: BackendsInfo,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/cluster_balance',
            name: 'Balance',
            component: Balance,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/BalanceInfo/:id',
            name: 'BalanceInfo',
            component: BalanceInfo,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/working_slots/:id',
            name: 'Working_slots',
            component: Working_slots,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/sched_stat/:id',
            name: 'sched_stat',
            component: sched_stat,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Priority_repair/:id',
            name: 'Priority_repair',
            component: Priority_repair,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/Pending_tablets/:id',
            name: 'Pending_tablets',
            component: Pending_tablets,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/running_tablets/:id',
            name: 'running_tablets',
            component: running_tablets,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/history_tablets/:id',
            name: 'history_tablets',
            component: history_tablets,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/query/Query',
            name: 'Query',
            component: Query,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/query/QueryProfile/:id',
            name: 'QueryProfile',
            component: QueryProfile,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/current_queries_info/:id',
            name: 'Current_queries_info',
            component: Current_queries_info,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/current_queries',
            name: 'Current_queries',
            component: Current_queries,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/session/Session',
            name: 'Session',
            component: Session,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/variable/Variable',
            name: 'Variable',
            component: Variable,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/syslog/SystemLog',
            name: 'SystemLog',
            component: SystemLog,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/ha/ha',
            name: 'HA',
            component: HA,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/dbs',
            name: 'dbs',
            component: dbs,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/dbs_info/:id',
            name: 'dbs_info',
            component: dbs_info,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/table_info/:id',
            name: 'table_info',
            component: table_info,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/partitions/:id',
            name: 'partitions',
            component: partitions,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/temp_partitions/:id',
            name: 'temp_partitions',
            component: temp_partitions,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/index_schema/:id',
            name: 'index_schema',
            component: index_schema,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/index_schema_struct/:id',
            name: 'index_schema_struct',
            component: index_schema_struct,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/partitions_index/:id',
            name: 'partitions_index',
            component: partitions_index,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/partitions_tablet/:id',
            name: 'partitions_tablet',
            component: partitions_tablet,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/dbs/partitions_tablet_info/:id',
            name: 'partitions_tablet_info',
            component: partitions_tablet_info,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/current_backend_instances',
            name: 'current_backend_instances',
            component: current_backend_instances,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/tasks',
            name: 'tasks',
            component: tasks,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/statistic',
            name: 'statistic',
            component: statistic,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/system/statistic_info/:id',
            name: 'statistic_info',
            component: statistic_info,
            meta: {
                requireAuth: true
            }
        }, {
            path: '/help/help',
            name: 'help',
            component: help,
            meta: {
                requireAuth: true
            }
        }]
    }]
})