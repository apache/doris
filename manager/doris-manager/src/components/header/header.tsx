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
import {LineChartOutlined, QuestionCircleOutlined,SettingOutlined } from '@ant-design/icons';
import SubMenu from 'antd/lib/menu/SubMenu';
import { Link } from 'react-router-dom';
import { IconSetting } from 'src/components/icons/setting.icon';
import React, { useEffect, useState } from 'react';
import { LayoutAPI } from './header.api';
import { useHistory } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import styles from './index.module.less';
import { ANALYTICS_URL } from '@src/common/common.data';

export function Header(props: any) {
    const { t } = useTranslation();
    const history = useHistory();
    const [statisticInfo, setStatisticInfo] = useState<any>({});
    const user = JSON.parse(window.localStorage.getItem('user') as string);
    function getCurrentUser() {
        LayoutAPI.getCurrentUser()
            .then(res => {
                window.localStorage.setItem('user', JSON.stringify(res.data))
                LayoutAPI.getSpaceName(res.data.space_id).then(res1 => {
                    setStatisticInfo(res1.data);
                })
            })
            .catch(err => {
                console.log(err);
            });
    }
    // useEffect(() => {
    //     getCurrentUser();
    // }, []);
    function clearAllCookie() {
        const keys = document.cookie.match(/[^ =;]+(?=\=)/g);
        if (keys) {
            for (let i = keys.length; i--; ) document.cookie = keys[i] + '=0;expires=' + new Date(0).toUTCString();
        }
    }
    function onAccountSettings() {
        history.push( `/${DEFAULT_NAMESPACE_ID}/user-setting`);
    }
    function onLogout() {
        LayoutAPI.signOut()
            .then(res => {
                console.log(res)
                if (res.code === 0) {
                    localStorage.removeItem('login');
                    history.push(`/login`);
                }
            })

    }
    const menu = (
        <Menu>
            <Menu.Item onClick={onAccountSettings}>{t`accountSettings`}</Menu.Item>
            <Menu.Item onClick={onLogout}>{t`Logout`}</Menu.Item>
        </Menu>
    );
    return (
        <div
            className={user.is_super_admin ? styles['adminStyle']: styles['userStyle']}
            style={{ padding: 0, background: user.is_super_admin ?  '#000' : '#f9fbfc', borderBottom: '1px solid #d9d9d9' }}
        > 
            <Row justify="space-between">
                {
                    user.is_super_admin ? (
                        <div
                            className={styles['logo']}
                        />
                    ) :(
                        <Col style={{ marginLeft: '2em' }}>
                            <span>{t`namespace`}：{statisticInfo.name}</span>
                        </Col>
                    )
                }
                
                <Col style={{ cursor: 'pointer', marginRight: 20, fontSize: 22 }}>
                    <Tooltip placement="bottom" title={t`backTo`+ "Studio"}>
                        <LineChartOutlined
                            className={styles['data-builder-header-items']}
                            style={{ marginRight: 20 , color: user.is_super_admin ?  '#fff' : '',}}
                            onClick={() => (window.location.href = `${window.location.origin}`)}
                        />
                    </Tooltip>
                    <Tooltip placement="bottom" title={t`Help`}>
                        <QuestionCircleOutlined
                            // className={styles['data-builder-header-items']}
                            style={{ marginRight: 20, color: user.is_super_admin ? '#fff' : '' }}
                            onClick={() => {
                                const analyticsUrl = `${window.location.origin}/docs/pages/产品概述/产品介绍.html`;
                                window.open(analyticsUrl);
                            }}
                        />
                    </Tooltip>
                    <Dropdown overlay={menu}>
                                <span onClick={e=> e.preventDefault()}>
                                    <SettingOutlined style={{ color: user.is_super_admin ? '#fff' : '' }}/>
                                </span>
                    </Dropdown>
                </Col>
            </Row>
        </div>
    );
}
