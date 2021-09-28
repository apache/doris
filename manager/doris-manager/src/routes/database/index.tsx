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

import React, { useState, useEffect } from 'react';
import styles from './database.module.less';
import CSSModules from 'react-css-modules';
import { useHistory } from 'react-router-dom';
import { Layout, Form, Tabs, Button, message } from 'antd';
import { HddOutlined } from '@ant-design/icons';
import { CommonHeader } from '@src/components/common-header/header';
import { DatabaseAPI } from './database.api';
import { DatabaseInfoResponse } from './database.interface';
import { useTranslation } from 'react-i18next';
import { getShowTime } from '@src/utils/utils';

const { Content, Sider } = Layout;
const iconDatabase = <HddOutlined />;
const { TabPane } = Tabs;
const layout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 18 },
};

function Database(props: any) {
    const history = useHistory();
    const { t } = useTranslation();
    const [dbName, setDbName] = useState<any>('');
    const [databaseInfo, setDatabaseInfo] = useState<DatabaseInfoResponse>({
        createTime: '',
        creator: '',
        describe: '',
        name: '',
    });
    useEffect(() => {
        refresh();
    }, [window.location.href]);
    function refresh() {
        const id = localStorage.getItem('database_id');
        const name = localStorage.getItem('database_name');
        if (!id) {
            return;
        }
        setDbName(name);
        DatabaseAPI.getDatabaseInfo({ dbId: id }).then(res => {
            const { msg, code, data } = res;
            if (code === 0) {
                setDatabaseInfo(res.data);
            } else {
                message.error(msg);
            }
        });
    }
    return (
        <Content styleName="database-main">
            <CommonHeader title={dbName} icon={iconDatabase} callback={refresh}></CommonHeader>
            <div styleName="database-content">
                <Tabs defaultActiveKey="1">
                    <TabPane tab={t`BasicInformationOfDatabase`} key="1">
                        <div styleName="database-content-des">
                            <Form {...layout} labelAlign="left">
                                <Form.Item label={t`DatabaseName`}>{databaseInfo.name}</Form.Item>
                                <Form.Item label={t`DatabaseDescriptionInformation`}>
                                    {databaseInfo.describe ? databaseInfo.describe : '-'}
                                </Form.Item>
                                <Form.Item label={t`CreationTime`}>{getShowTime(databaseInfo.createTime) ? getShowTime(databaseInfo.createTime) : '-'}</Form.Item>
                            </Form>
                        </div>
                    </TabPane>
                </Tabs>
            </div>
        </Content>
    );
}

export default CSSModules(styles)(Database);
