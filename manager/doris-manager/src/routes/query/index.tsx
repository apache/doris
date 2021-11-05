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

/* eslint-disable prettier/prettier */
import React, { useState, useEffect } from 'react';
import { Table, Card, Space, Input } from 'antd';
import { useTranslation } from 'react-i18next';
import { getQueryInfo } from './query.api';
import { ColumnsType } from 'antd/es/table';
import { FILTERS } from './query.data';
import { useHistory } from 'react-router-dom';
import { ClippedText } from '@src/components/clipped-text/clipped-text';
const { Search } = Input;

interface ICol {
    [propName: string]: any;
}

function Query() {
    const { t } = useTranslation();
    const history = useHistory();
    const [tableData, setTableData] = useState<ICol[]>([]);
    const [tableColumn, setTableColumn] = useState<ColumnsType<ICol>>([]);

    function queryDetails(record: any) {
        console.log(record)
        history.push(`/details/${record[0]}`)
    }
    useEffect(() => {
        getQueryInfo().then(res => {
            const { column_names, rows } = res.data;
            handleResult(column_names, rows);
        });
    }, []);
    const onSearch = (value: string) => {
        let newValue = encodeURI(value)
        getQueryInfo({
            search: newValue,
        }).then(res => {
            const { column_names, rows } = res.data;
            handleResult(column_names, rows);
        });
    };

    const handleResult = (column_names: string[], rows: [][]) => {
        const columns = column_names.map((item: string, index: number) => {
            if (item === 'Query ID') {
                return {
                    title: item,
                    dataIndex: `${index}`,
                    key: item,
                    columnWidth: 10,
                    render: (text: any, record: any, index: any) => (
                        <ClippedText text={text}>
                            <Space size="middle">
                                <a onClick={() => queryDetails(record)}>{text}</a>
                            </Space>
                        </ClippedText>
                        
                    ),
                };
            }
            if (item === '状态') {
                return {
                    title: item,
                    dataIndex: `${index}`,
                    key: item,
                    columnWidth: 10,
                    filters: FILTERS,
                    onFilter: (value: any, record: any) => record[index].indexOf(value) === 0,
                };
            }
            if (item === 'FE节点') {
                return {
                    title: item,
                    dataIndex: index,
                    key: item,
                    width: 150,
                    ellipsis: true,
                };
            }
            if (item === '开始时间') {
                return {
                    title: item,
                    dataIndex: index,
                    key: item,
                    width: 150,
                    ellipsis: true,
                };
            }
            if (item === '结束时间') {
                return {
                    title: item,
                    dataIndex: index,
                    key: item,
                    width: 150,
                    ellipsis: true,
                };
            }
            return {
                title: item,
                dataIndex: index,
                key: item,
                columnWidth: 10,
                ellipsis: true,
            };
        });
        setTableColumn(columns);
        setTableData(rows.map((item: string[]) => ({ ...item })));
    };

    return (
        <>
            <Card
                style={{ width: '100%' }}
                title={t`queryProfileFirst`}
                extra={<Search placeholder="搜索" onSearch={onSearch} allowClear/>}
            >
                <Table<ICol>
                    columns={tableColumn}
                    dataSource={tableData}
                    bordered
                    scroll={{ x: 1300 }}
                    size="middle"
                    pagination={{
                        pageSizeOptions: ['10', '20', '50'],
                    }}
                    locale={{emptyText: t`noDate` }}
                ></Table>
            </Card>
        </>
    );
}

export default Query;
