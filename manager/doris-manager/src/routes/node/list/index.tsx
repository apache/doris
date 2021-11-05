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
import React, { useEffect, useState } from 'react';
import { Table, Space, message } from 'antd';
import { NodeAPI } from './node.api';
import { useHistory } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
function NodeList() {
    const history = useHistory();
    const { t } = useTranslation();
    const [feColumns, setFeColumns] = useState<any>([]);
    const [beColumns, setBeColumns] = useState<any>([]);
    const [brokersColumns, setBrokersColumns] = useState<any>([]);
    const [beTableData, setBeTableData] = useState<any>([]);
    const [feTableData, setFeTableData] = useState<any>([]);
    const [brokersTableData, setBrokersTableData] = useState<any>([]);
    function refresh() {
        NodeAPI.getFrontends().then(res => {
            const { msg, data, code } = res;
            const { column_names, rows } = data;
            if (code === 0) {
                const column = column_names.map((item: string, index: number) => {
                    if (index === 0) {
                        return {
                            title: 'FE '+t`Node`,
                            dataIndex: index,
                            key: item,
                            width: 200,
                        };
                    } else {
                        return {
                            title: item,
                            dataIndex: index,
                            key: item,
                            width:200,
                        };
                    }
                });
                column.push({
                    title: 'CONF',
                    key: 'action',
                    render: (text: any, record: any, index: any) => (
                        <Space size="middle">
                            <a onClick={() => toFeDetailsPage(record)}>{t`Details`}</a>
                        </Space>
                    ),
                    fixed: 'right',
                    width:100,
                });
                setFeColumns(column);
                setFeTableData(rows.map((item: string[]) => ({ ...item })));
            } else {
                message.error(msg);
            }
        });
        NodeAPI.getBrokers().then(res => {
            const { msg, data, code } = res;
            const { column_names, rows } = data;
            if (code === 0) {
                const column = column_names.map((item: string, index: number) => {
                    if (index === 0) {
                        return {
                            title: 'Brokers',
                            dataIndex: index,
                            key: item,
                            width: 200,
                        };
                    } else {
                        return {
                            title: item,
                            dataIndex: index,
                            key: item,
                            width:200,
                        };
                    }
                });
                setBrokersColumns(column);
                setBrokersTableData(rows.map((item: string[]) => ({ ...item })));
            } else {
                message.error(msg);
            }
        });
        NodeAPI.getBackends().then(res => {
            const { msg, data, code } = res;
            const { column_names, rows } = data;
            if (code === 0) {
                const column = column_names.map((item: string, index: number) => {     
                    if (index === 0) {
                        return {
                            title: 'BE '+t`Node`,
                            dataIndex: index,
                            key: item,
                            width: 200,
                        };
                    } else {
                        return {
                            title: item,
                            dataIndex: index,
                            key: item,
                            width:200,
                        };
                    }
                }
                );
                column.push({
                    title: 'CONF',
                    key: 'CONF',
                    render: (text: any, record: any, index: any) => (
                        <Space>
                            <a onClick={() => toBeDetailsPage(record)} >{ t`Details`}</a>
                        </Space>
                    ),
                    fixed: 'right',
                    width:100,
                });
                setBeColumns(column);
                setBeTableData(rows.map((item: string[]) => ({ ...item })));
            } else {
                message.error(msg);
            }
        });
    }
    useEffect(() => {
        refresh();
    }, []);

    function toBeDetailsPage(record: any) {
        let node = "";
        node += record[2]+":"+record[6]
        localStorage.setItem("nodeText", node);
        history.push(`/configuration/be`)
    }

    function toFeDetailsPage(record: any) {
        let node = "";
        node += record[1]+":"+record[4]
        localStorage.setItem("nodeText", node);
        history.push(`/configuration/fe`) ;
    }
    return (
        <>
            <Table
                tableLayout="fixed"
                columns={feColumns}
                dataSource={feTableData}
                bordered
                scroll={{ x: 1300 }}
                size="middle"
                style={{ padding: 40 }}
                pagination={false}
            ></Table>
            <Table
                tableLayout="fixed"
                columns={beColumns}
                dataSource={beTableData}
                bordered
                scroll={{ x: 1300 }}
                size="middle"
                style={{ padding: 40 }}
                pagination={false}
            ></Table>
            <Table
                tableLayout="fixed"
                columns={brokersColumns}
                dataSource={brokersTableData}
                bordered
                scroll={{ x: 1300 }}
                size="middle"
                style={{ padding: 40 }}
                pagination={false}
            ></Table>
        </>
    );
}
export default NodeList;
