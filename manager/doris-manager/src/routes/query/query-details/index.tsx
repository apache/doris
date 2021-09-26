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

import React, { useState, useEffect, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Table, Card } from 'antd';
import { getSQLText, getProfileText, getQueryInfo } from './../query.api';
import Profile from './profile';
import styles from './query.module.less';
import { ColumnsType } from 'antd/es/table';
import { ClippedText } from '@src/components/clipped-text/clipped-text';
import hljs from 'highlight.js'
import './code.css';
type tabType = 'sql' | 'text' | 'profile';
interface ICol {
    [propName: string]: any;
}
function QueryDetails() {
    const [currentTab, setCurrentTab] = useState<tabType>('sql');
    const [currentSQL, setCurrentSQL] = useState('sql');
    const [currentText, setCurrentText] = useState('sql');
    const [tableData, setTableData] = useState([]);
    const [tableColumn, setTableColumn] = useState<ColumnsType<ICol>>([]);
    const params = useParams<{ queryId: string }>();
    const queryId = params.queryId;
    const tabListNoTitle = [
        {
            key: 'sql',
            tab: 'SQL语句',
        },
        {
            key: 'text',
            tab: 'Text',
        },
        {
            key: 'profile',
            tab: 'Visualization',
        },
    ];

    const contentListNoTitle = {
        sql: (
            <div className={styles.textBox}>
                <div dangerouslySetInnerHTML={{__html:hljs.highlight(currentSQL, {language: 'sql'}).value}}></div>
            </div>
        ),
        text: (
            <div className={styles.textBox}>
                <pre>{currentText}</pre>
            </div>
        ),
        profile: <Profile></Profile>,
    };

    useEffect(() => {
        getQueryInfo({ query_id: queryId }).then(res => {
            const { column_names, rows } = res.data;
            const columns = column_names.map((item: string, index: number) => {
                if (item === 'Query ID') {
                    return {
                        title: item,
                        dataIndex: `${index}`,
                        key: item,
                        columnWidth: 10,
                        render: (text: any, record: any, index: any) => (
                            <ClippedText>{text}</ClippedText>
                        ),
                    };
                }
                return {
                    title: item,
                    dataIndex: index,
                    key: item,
                    columnWidth: '10px',
                    ellipsis: true,
                };
            });
            setTableColumn(columns);
            setTableData(rows.map((item: string[]) => ({ ...item })));
        });
    }, []);
    useEffect(() => {
        getSQLText({ queryId }).then(res => {
            setCurrentSQL(res.data?.sql || '--');
        });
        getProfileText({ queryId }).then(res => {
            setCurrentText(res.data?.profile || '--');
        });
    }, []);

    return (
        <>
            <Card style={{ width: '100%', border: 'none' }} bordered={false}>
                <Table columns={tableColumn} dataSource={tableData} size="middle" pagination={false}></Table>
            </Card>
            <Card
                style={{ width: '100%', border: 'none' }}
                tabList={tabListNoTitle}
                activeTabKey={currentTab}
                onTabChange={key => {
                    setCurrentTab(key as tabType);
                }}
            >
                {contentListNoTitle[currentTab]}
            </Card>
        </>
    );
}

export default QueryDetails;
