/** @format */

import React, { useState, useEffect, useCallback } from 'react';
import styles from './table-content.module.less';
import CSSModules from 'react-css-modules';
import { Layout, Menu, Tabs, Button } from 'antd';
import { TableOutlined } from '@ant-design/icons';
import { CommonHeader } from '@src/components/common-header/header';

import { TableInfoResponse } from './table.interface';
import { TableAPI } from './table.api';
import { useHistory } from 'react-router-dom';

import BaseInfo from './tabs/baseInfo';
import { Schema } from './schema/schema';
import DataImportTab from './tabs/data-import';
import { TableInfoTabTypeEnum } from './table-content.data';
import { Redirect, Route, Switch } from 'react-router-dom';
import { pathToRegexp } from 'path-to-regexp';
import DataPreview from './tabs/data.pre';
const { Content, Sider } = Layout;
const iconTable = <TableOutlined />;
import { isTableIdSame } from '@src/utils/utils';
const { TabPane } = Tabs;
let id: any = '',
    name: any = '';
function TableContent(props: any) {
    console.log(props)
    const history = useHistory();
    const { match } = props;
    const [dbId, setDbId] = useState<any>();
    const matchedPath = pathToRegexp(`${match.path}/:tabType`).exec(props.location.pathname);
    useEffect(() => {
        id = localStorage.getItem('table_id');
        name = localStorage.getItem('table_name');
        setDbId(localStorage.getItem('database_id'));
        isTableIdSame();
    }, [window.location.href]);

    function refresh(router: any) {
        //
    }

    function handleTabChange(activeTab: string) {
        props.history.push({
            pathname: `${match.url}/${activeTab}`,
            state: { id: id, name: name },
        });
    }

    return (
        <Content styleName="table-main">
            <CommonHeader title={name} icon={iconTable} callback={refresh}></CommonHeader>
            <div styleName="table-content">
                <Tabs
                    // defaultActiveKey={`${ma}`}
                    activeKey={matchedPath ? matchedPath[3] : TableInfoTabTypeEnum.BasicInfo}
                    onChange={(activeTab: string) => handleTabChange(activeTab)}
                >
                    <TabPane tab="基本信息" key={TableInfoTabTypeEnum.BasicInfo}>
                        {/* <BaseInfo tableInfo={tableInfo} /> */}
                    </TabPane>
                    <TabPane tab="数据预览" key={TableInfoTabTypeEnum.DataPreview}>
                        {/* 12312 */}
                    </TabPane>
                    <TabPane tab="Schema" key={TableInfoTabTypeEnum.Schema}></TabPane>
                </Tabs>
                <Switch>
                    <Route
                        path={`${match.path}/${TableInfoTabTypeEnum.BasicInfo}`}
                        render={props => <BaseInfo tableId={match.params.tableId} />}
                    />
                    <Route
                        path={`${match.path}/${TableInfoTabTypeEnum.DataPreview}`}
                        render={props => <DataPreview tableId={match.params.tableId} />}
                    />
                    <Route
                        path={`${match.path}/${TableInfoTabTypeEnum.DataImport}`}
                        render={props => <DataImportTab tableId={match.params.tableId} tableName={name} />}
                    />
                    <Route
                        path={`${match.path}/${TableInfoTabTypeEnum.Schema}`}
                        render={props => <Schema tableId={match.params.tableId} />}
                    />
                    <Redirect
                        to={{
                            pathname: `${match.path}/${TableInfoTabTypeEnum.BasicInfo}`,
                            state: { id: id, name: name },
                        }}
                    />
                </Switch>
            </div>
        </Content>
    );
}

export default CSSModules(styles)(TableContent);
