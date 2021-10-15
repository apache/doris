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
import styles from './tabs.module.less';
import CSSModules from 'react-css-modules';
import { Table, Tag, Button, Row, Col, Modal, message, Empty } from 'antd';
import { useHistory } from 'react-router-dom';
import EventEmitter from '@src/utils/event-emitter';
import ImportMenu from './dataImport-menu';
import { TableAPI } from './tabs.api';
import { getShowTime } from '../../../utils/utils';
const columns = [
    {
        title: '任务名称',
        dataIndex: 'taskName',
        key: 'taskName',
        width: 250,
        // render: (text: any) => <a>{text}</a>,
    },
    {
        title: '任务类型',
        dataIndex: 'importType',
        key: 'importType',
        width: 100,
        render: function (text: any, record: any) {
            return <span>{text === 'file' ? '本地文件导入' : 'hdfs文件导入'}</span>;
        },
    },
    {
        title: '创建人',
        dataIndex: 'creator',
        key: 'creator',
        width: 110,
    },
    {
        title: '创建时间',
        key: 'createTime',
        dataIndex: 'createTime',
        width: 150,
        render: function (text: any, record: any) {
            return <span>{getShowTime(record.createTime)}</span>;
        },
    },
    {
        title: '任务状态',
        dataIndex: 'status',
        key: 'status',
        width: 70,
        render: function (text: any, record: any) {
            return <Tag color={STATUS_TYPES[record.status]?.key}>{STATUS_TYPES[record.status]?.value}</Tag>;
        },
    },
];
const STATUS_TYPES = {
    PENDING: {
        key: 'warning',
        value: '等待执行',
    },
    LOADING: {
        key: 'processing',
        value: '执行中',
    },
    Success: { key: 'success', value: '已完成' },
    FINISHED: { key: 'success', value: '已完成' },
    CANCELLED: { key: 'error', value: '失败' },
    Fail: { key: 'error', value: '失败' },
};

function DataImportTab(props: any) {
    const [tableData, setTableData] = useState([]);
    const [total, setTotal] = useState();
    const [selectItem, setSelectItem] = useState<any>({});
    const [isModalVisible, setIsModalVisible] = useState(false);

    useEffect(() => {
        refresh({});
        const unListen = EventEmitter.on('refreshData', () => {
            refresh;
        });
        return unListen;
    }, []);
    function refresh(pageInfo: any) {
        TableAPI.getTableList({
            tableId: localStorage.getItem('table_id'),
            page: pageInfo.current ? pageInfo.current : 1,
            pageSize: 10,
        }).then(res => {
            const { msg, code, data } = res;
            if (code === 0) {
                setTableData(data.taskRespList);
                setTotal(data.totalSize);
            } else {
                setTableData([]);
                message.error(msg);
            }
        });
    }
    function rowClick(e: any, record: any) {
        setSelectItem(record);
        showModal();
    }

    const showModal = () => {
        setIsModalVisible(true);
    };

    const handleCancel = () => {
        setIsModalVisible(false);
    };
    function getShowUrl(str: string) {
        if (str && str.indexOf('fileUrl') >= 0) {
            str.split(';')[0].split('Url:')[1];
        } else {
            return str;
        }
    }
    return (
        <div styleName="dataImport-content-des">
            <Row>
                <ImportMenu />
            </Row>

            <Table
                bordered
                columns={columns}
                rowKey="taskName"
                dataSource={tableData}
                scroll={{ x: '500', y: '49vh' }}
                className={styles['import-table']}
                onRow={record => {
                    return {
                        onClick: event => {
                            rowClick(event, record);
                        },
                    };
                }}
                pagination={{ total: total }}
                onChange={pagination => {
                    refresh(pagination);
                }}
                size="small"
                locale={{
                    emptyText: (
                        <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={<span>未发现数据导入任务</span>} />
                    ),
                }}
            />
            <Modal
                title={'任务名称： ' + selectItem.taskName}
                visible={isModalVisible}
                className={styles['import-item-modal']}
                width="600px"
                footer={
                    [] // 设置footer为空，去掉 取消 确定默认按钮
                }
                onCancel={handleCancel}
            >
                <Row>
                    <Col span={6} className={styles['label']}>
                        传输类型：
                    </Col>
                    <Col>{selectItem.importType === 'hdfs' ? '文件系统导入' : '本地上传文件'}</Col>
                </Row>
                <Row>
                    <Col span={6} className={styles['label']}>
                        数据源：
                    </Col>
                    <Col span={18}>
                        {selectItem.fileInfo && selectItem.fileInfo.indexOf('fileUrl') >= 0
                            ? selectItem.fileInfo.split(';')[0].split('Url:')[1]
                            : selectItem.fileInfo}
                    </Col>
                </Row>
                <Row>
                    <Col span={6} className={styles['label']}>
                        数据目的地：
                    </Col>
                    <Col>{localStorage.getItem('database_name') + '.' + localStorage.getItem('table_name')}</Col>
                </Row>
                <Row>
                    <Col span={6} className={styles['label']}>
                        任务创建人：
                    </Col>
                    <Col>{selectItem.creator}</Col>
                </Row>
                <Row>
                    <Col span={6} className={styles['label']}>
                        创建时间：
                    </Col>
                    <Col>{getShowTime(selectItem.createTime)}</Col>
                </Row>
                <Row>
                    <Col span={6} className={styles['label']}>
                        任务状态：
                    </Col>
                    <Col span={14}>
                        <Tag color={STATUS_TYPES[selectItem.status]?.key}>{STATUS_TYPES[selectItem.status]?.value}</Tag>
                    </Col>
                    {selectItem.status === 'CANCELLED' || selectItem.status === 'Fail' ? (
                        <Col>
                            <Tag
                                color={STATUS_TYPES[selectItem.status]?.key}
                                style={{
                                    border: 'none',
                                    background: 'none',
                                    marginTop: '7px',
                                }}
                            >
                                <span
                                    style={{ textDecoration: 'underline', cursor: 'pointer' }}
                                    onClick={() => {
                                        if (selectItem.errorMsg) {
                                            window.open(selectItem.errorMsg);
                                        } else {
                                            message.error(selectItem.errorInfo);
                                        }
                                    }}
                                >
                                    任务详情
                                </span>
                            </Tag>
                        </Col>
                    ) : (
                        ''
                    )}
                </Row>
            </Modal>
        </div>
    );
}

export default CSSModules(styles)(DataImportTab);
