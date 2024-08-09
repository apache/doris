/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, {useEffect, useState} from 'react';
import {AdHocAPI} from 'Src/api/api';
import {getDbName} from 'Utils/utils';
import {notification, Row, Table} from 'antd';
import {FlatBtn} from 'Components/flatbtn';
import {useTranslation} from 'react-i18next';

export function DataPrev(props: any) {
    let {t} = useTranslation();
    const {db_name, tbl_name} = getDbName();
    const dbName = db_name.replace(/default_cluster:/, '')
    const [tableData, setTableData] = useState<any[]>([]);
    const [columns, setColumns] = useState<any[]>([]);

    function toQuery(): void {
        if (!tbl_name) {
            notification.error({message: t<string>('selectWarning')});
            return;
        }
        AdHocAPI.doQuery({
            db_name,
            body: {stmt: `SELECT * FROM ${dbName}.${tbl_name} LIMIT 10`},
        }).then((res: any) => {
            if (res && res.msg === 'success') {
                console.log(getColumns(res.data?.meta), 2222)
                setColumns(getColumns(res.data?.meta))
                setTableData(getTableData(res.data));
            }
        }).catch(() => {
            setTableData([]);
        });
    }

    function getColumns(params: any[]) {
        if (!params || params.length === 0) {
            return [];
        }

        let arr: any[] = params.map(item => {
            return {
                title: item.name,
                dataIndex: item.name,
                key: item.name,
                width: 150,
                render: (text, record, index) => {
                    return text === '\\N' ? '-' : text
                }
            };
        });
        return arr;
    }

    function getTableData(data): any[] {
        let meta = data.meta;
        let source = data.data;
        let res: any[] = [];
        if (!source || source.length === 0) {
            return [];
        }
        let metaArr = meta.map(item => item.name)
        for (let i = 0; i < source.length; i++) {
            let node = source[i];
            if (node.length !== meta.length) {
                return []
            }
            let obj = {}
            metaArr.map((item, idx) => {
                obj[item] = node[idx]
            })
            obj['key'] = i
            res.push(obj)
        }
        return res;
    }

    useEffect(() => {
        toQuery();
    }, [location.pathname]);
    return (
        <div>
            <Row justify="space-between" style={{marginBottom: 10}}>
                <span style={{paddingBottom: '15px'}}>{t('dataPreview') + "(" + t('display10') + ")"}</span>
                <span>
                    {dbName}.{tbl_name}
                </span>
                <FlatBtn
                    onClick={() =>
                        toQuery()
                    }
                >
                    {t('refresh')}
                </FlatBtn>
            </Row>
            <Table
                bordered
                columns={columns}
                style={{maxWidth: ' calc(100vw - 350px)'}}
                // scroll={{ x:'500', y: '36vh'}}
                scroll={{x: 1500, y: 300}}
                dataSource={tableData}
                size="small"
            />
        </div>
    );
}

 
