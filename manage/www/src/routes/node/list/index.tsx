/* eslint-disable prettier/prettier */
import React, { useEffect, useState } from 'react';
import { Table, Space, message } from 'antd';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import { NodeAPI } from './node.api';
import { useHistory } from 'react-router-dom';
function NodeList() {
    const history = useHistory();
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
                            title: 'FE节点',
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
                            <a onClick={() => toFeDetailsPage(record)}>详情</a>
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
                            title: 'BE节点',
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
                        <a onClick={() => toBeDetailsPage(record)} >详情</a>
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
        history.push(`/${DEFAULT_NAMESPACE_ID}/be-configuration`)
    }

    function toFeDetailsPage(record: any) {
        let node = "";
        node += record[1]+":"+record[4]
        localStorage.setItem("nodeText", node);
        history.push(`/${DEFAULT_NAMESPACE_ID}/fe-configuration`) ;
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
