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
import { Form, message, Table, Empty } from 'antd';
import { useHistory } from 'react-router-dom';
import { TableAPI } from '../table.api';
import EventEmitter from '@src/utils/event-emitter';
const layout = {
    labelCol: { span: 6 },
    wrapperCol: { span: 18 },
};

function DataPre(props: any) {
    // const history = useHistory();
    const [tableInfo, setTableInfo] = useState<any>([]);
    const [columns, setColumns] = useState([]);
    useEffect(() => {
        refresh(props.tableId);
        const unListen = EventEmitter.on('refreshData', () => {
            refresh(props.tableId);
        });
        return unListen;
    }, []);
    function refresh(id: string) {
        TableAPI.sendSql({
            nsId: '0',
            dbId: localStorage.getItem('database_id'),
            data: `select * from ${localStorage.getItem('table_name')} limit 50`,
        }).then(res => {
            const { msg, code, data } = res;
            if (code === 0) {
                setColumns(getColumns(res.data.meta));
                setTableInfo(getTable(res.data.data, res.data.meta));
            } else {
                message.error(msg);
            }
            // setTableInfo(res.data);
        });
    }
    function getColumns(arr: any) {
        return arr.map((item: any) => {
            return {
                title: item.name,
                dataIndex: item.name,
                key: item.name,
                width: 150,
            };
        });
    }
    function getTable(source: any, meta: any) {
        const res = [];
        if (!source || source.length === 0) {
            return [];
        }
        const metaArr = meta.map((item: any) => item.name);
        for (let i = 0; i < source.length; i++) {
            const node = source[i];
            if (node.length !== meta.length) {
                return {};
            }
            const obj = {};
            metaArr.map((item: any, idx: any) => {
                obj[item] = node[idx];
            });
            obj['key'] = i;
            res.push(obj);
        }
        return res;
    }
    return (
        <div styleName="table-content-des">
            <Table
                columns={columns}
                scroll={{ x: 'max-content', y: 'calc(100vh - 400px)' }}
                className={styles['import-table']}
                bordered={true}
                size="small"
                dataSource={tableInfo}
                locale={{
                    emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={<span>当前数据表为空</span>} />,
                }}
            />
        </div>
    );
}

export default CSSModules(styles)(DataPre);
