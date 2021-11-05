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

import React from 'react';
import { Menu, Dropdown, Button } from 'antd';
import { UploadOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router-dom';
import { FileWordOutlined, CloudServerOutlined } from '@ant-design/icons';
import styles from '../tabs.module.less';
const getMenu = (history: any, props: any) => {
    return (
        <Menu>
            <Menu.Item>
                <a
                    style={{ padding: '10px 25px', textAlign: 'center', color: '#757272' }}
                    onClick={() => {
                        history.push({
                            pathname: `/local-import/${localStorage.table_id}`,
                        });
                    }}
                >
                    <FileWordOutlined style={{ paddingRight: '15px', fontSize: '20px', verticalAlign: 'middle' }} />
                    本地上传文件
                </a>
            </Menu.Item>
            <Menu.Item>
                <a
                    style={{ padding: '10px 25px', textAlign: 'center', color: '#757272' }}
                    onClick={() => {
                        history.push({
                            pathname: `/system-import/${localStorage.table_id}`,
                        });
                    }}
                >
                    <CloudServerOutlined style={{ paddingRight: '15px', fontSize: '20px', verticalAlign: 'middle' }} />
                    文件系统导入
                </a>
            </Menu.Item>
        </Menu>
    );
};

function ImportMenu(props: any) {
    const history = useHistory();
    const MENU = getMenu(history, props);

    return (
        <div style={{ width: '100%' }}>
            <Dropdown overlay={MENU} placement="bottomCenter" overlayClassName={styles['import-drop']}>
                <Button
                    type="primary"
                    style={{ margin: '0 10px 15px', float: 'right' }}
                    shape="round"
                    icon={<UploadOutlined />}
                >
                    导入数据
                </Button>
            </Dropdown>
        </div>
    );
}

export default ImportMenu;
