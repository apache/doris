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

import { Layout, Menu, Tooltip, Col, Row, Dropdown, message, Anchor } from 'antd';
import Sider from 'antd/lib/layout/Sider';
import SubMenu from 'antd/lib/menu/SubMenu';
import { LineChartOutlined, QuestionCircleOutlined, UserOutlined } from '@ant-design/icons';
import { Link, useHistory, useRouteMatch } from 'react-router-dom';
import React, { useEffect, useState } from 'react';

import { useTranslation } from 'react-i18next';
import styles from './sidebar.less';

export function Sidebar(props: any) {
    const { t } = useTranslation();
    const [statisticInfo, setStatisticInfo] = useState<any>({});
    const [selectedKeys, setSelectedKeys] = useState('/dashboard/overview');

    const history = useHistory();
    useEffect(() => {
        if (history.location.pathname.includes('configuration')) {
            setSelectedKeys('/configuration');
        } else {
            setSelectedKeys(history.location.pathname);
        }
    }, [history.location.pathname]);

    return (
        <Sider width={200} className="DAE-manager-side">
            <Menu
                mode="inline"
                theme="dark"
                defaultSelectedKeys={['/dashboard/overview']}
                defaultOpenKeys={['nodes']}
                selectedKeys={[selectedKeys]}
                style={{ height: '100%', borderRight: 0 }}
            >
                <Menu.Item style={{ paddingLeft: '8px', marginTop: '10px' }}>
                    <div
                        className={styles['logo']}
                        onClick={() => props.history.push(`/meta/index`)}
                    />
                </Menu.Item>
                <Menu.Item key="2">
                    <Link to={`/meta`}>{t`data`}</Link>
                </Menu.Item>
                <SubMenu key="nodes" title="节点">
                    <Menu.Item key={`/list`}>
                        <Link to={`/list`}>列表</Link>
                    </Menu.Item>
                    <Menu.Item key={`/configuration`}>
                        <Link to={`/configuration`}>配置项</Link>
                    </Menu.Item>
                    <Menu.Item key={`/dashboard/overview`}>
                        <Link to={`/dashboard/overview`}>仪表盘</Link>
                    </Menu.Item>
                </SubMenu>
                <SubMenu key="cluster" title="集群">
                    <Menu.Item key="/cluster/monitor">
                        <Link to={`/cluster/monitor`}>监控</Link>
                    </Menu.Item>
                    {/* <Menu.Item key="/cluster/list">
                        <Link to={`/cluster/list`}>列表</Link>
                    </Menu.Item> */}
                </SubMenu>
                <Menu.Item key="/query">
                    <Link to={`/query`}>查询</Link>
                </Menu.Item>
                {/* </SubMenu> */}
            </Menu>
        </Sider>
    );
}

