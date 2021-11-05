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

/** @format */

import React, { useState, useCallback } from 'react';
import styles from './create.module.less';
// import CSSModules from 'react-css-modules';
import { Menu, Dropdown, Button, Space } from 'antd';
import { TableOutlined, HddOutlined, PlusOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router-dom';
import DatabaseModal from './databaseModal';
const getMenu = (history: any, setIsShow: any) => {
    return (
        <Menu>
            <Menu.Item
                onClick={() => {
                    setIsShow(true);
                }}
            >
                <a style={{ padding: '15px 0', textAlign: 'center', color: '#757272' }}>
                    <HddOutlined style={{ paddingRight: '8px', fontSize: '20px', verticalAlign: 'middle' }} />
                    创建数据库
                </a>
            </Menu.Item>
            <Menu.Item
                onClick={() => {
                    history.push(`/new-table`);
                }}
            >
                <a style={{ padding: '15px 0', textAlign: 'center', color: '#757272' }}>
                    <TableOutlined style={{ paddingRight: '8px', fontSize: '20px', verticalAlign: 'middle' }} />
                    创建数据表
                </a>
            </Menu.Item>
        </Menu>
    );
};

function CreateMenu() {
    const history = useHistory();
    const [isShow, setIsShow] = useState();
    const MENU = getMenu(history, setIsShow);

    function refresh() {
        console.log(1111);
    }
    return (
        <div style={{ width: '100%' }}>
            <Dropdown overlay={MENU} placement="bottomCenter" overlayClassName={styles['create-drop']}>
                <Button icon={<PlusOutlined />} type="primary" style={{ width: '100%', borderRadius: '10px' }}>
                    创建数据
                </Button>
            </Dropdown>
            <DatabaseModal
                isShow={isShow}
                changeShow={(e: any) => {
                    setIsShow(e);
                }}
            />
        </div>
    );
}

export default CreateMenu;
