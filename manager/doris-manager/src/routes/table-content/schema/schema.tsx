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

import { useRequest } from '@umijs/hooks';
import { Form, Row, Table } from 'antd';
import { ColumnsType } from 'antd/lib/table';
import React, { useState, useEffect } from 'react';
import { TableTypeEnum, TABLE_TYPE_KEYS } from '@src/common/common.data';
import { LoadingWrapper } from '@src/components/loadingwrapper/loadingwrapper';
import { IResult } from 'src/interfaces/http.interface';
import { isSuccess } from '@src/utils/http';
import { SchemaAPI } from './schema.api';
import EventEmitter from '@src/utils/event-emitter';
import { useTranslation } from 'react-i18next';
import styles from '../tabs/tabs.module.less';

export function Schema(props: any) {
    const {t } = useTranslation()
    const [dataSource, setDataSource] = useState([]);
    const [columns, setColumns] = useState<ColumnsType<any>>([]);
    const [tableType, setTableType] = useState('');
    const { tableId } = props;
    useEffect(() => {
        const unListen = EventEmitter.on('refreshData', () => {
            // refresh(props.tableId);
        });
        return unListen;
    }, []);
    const { loading } = useRequest<IResult<any>>(() => SchemaAPI.getSchema(tableId), {
        refreshDeps: [tableId],
        onSuccess: async (res: any) => {
            if (isSuccess(res)) {
                const {
                    TABLE_COLUMN_DUPLICATE,
                    TABLE_COLUMN_AGGREGATE,
                    TABLE_COLUMN_UNIQUE,
                    BASIC_COLUMN,
                } = await import('./schema.data');
                setDataSource(res.data.schema);
                const type =
                    TABLE_TYPE_KEYS.filter(tableType => tableType.value === res.data.keyType)[0]?.text || '元数据表';
                setTableType(type);
                switch (res.data.keyType) {
                    case TableTypeEnum.AGG_KEYS:
                        setColumns(TABLE_COLUMN_AGGREGATE);
                        break;
                    case TableTypeEnum.DUP_KEYS:
                        setColumns(TABLE_COLUMN_DUPLICATE);
                        break;
                    case TableTypeEnum.UNIQUE_KEYS:
                        setColumns(TABLE_COLUMN_UNIQUE);
                        break;
                    default:
                        setColumns(BASIC_COLUMN);
                        break;
                }
            }
        },
    });
    return (
        <div>
            <Row className={styles['schema-row']}>
                <Form.Item label={t`TableType`}>
                    <span className="ant-form-text">{tableType}</span>
                </Form.Item>
            </Row>
            <Row className={styles['schema-row']}>
                <Form.Item label={t`TableStructure`}></Form.Item>
            </Row>
            <LoadingWrapper loading={loading}>
                <Table
                    bordered
                    columns={columns}
                    dataSource={dataSource}
                    rowKey="field"
                    size="small"
                    scroll={{ x: 'max-content', y: 'calc(100vh - 500px)' }}
                    className={styles['import-table']}
                />
            </LoadingWrapper>
        </div>
    );
}
