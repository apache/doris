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
                    <TabPane tab="数据库基本信息" key="1">
                        <div styleName="database-content-des">
                            <Form {...layout} labelAlign="left">
                                <Form.Item label="数据库名称">{databaseInfo.name}</Form.Item>
                                <Form.Item label="数据库描述信息">
                                    {databaseInfo.describe ? databaseInfo.describe : '-'}
                                </Form.Item>
                                <Form.Item label="创建时间">{getShowTime(databaseInfo.createTime) ? getShowTime(databaseInfo.createTime) : '-'}</Form.Item>
                            </Form>
                        </div>
                    </TabPane>
                </Tabs>
            </div>
        </Content>
    );
}

export default CSSModules(styles)(Database);
