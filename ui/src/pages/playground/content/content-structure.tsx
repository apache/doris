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
 
import React, {useState,useEffect} from 'react';
import {Tabs, Row, Table, notification} from 'antd';
import {TabPaneType,QUERY_TABTYPE} from '../adhoc.data';
import React from 'react';
import {FlatBtn} from 'Components/flatbtn';
import {TABLE_DELAY} from 'Constants';
import {useRequest} from '@umijs/hooks';
import {AdHocAPI} from 'Src/api/api';
import {getDbName} from 'Utils/utils';
import {Result} from '@src/interfaces/http.interface';
import {DataPrev} from './components/data-prev';
import getColumns from './getColumns';
import {useHistory} from 'react-router-dom';
import {useTranslation} from 'react-i18next';
export function ContentStructure(props: any) {
    let { t } = useTranslation()
    const {db_name,tbl_name}=getDbName();
    const [columns, setColumns] = useState([])
    const getTableInfoRequest = useRequest<Result<any>>(
        () => {
            const param = {
                db_name,
                tbl_name,
            };
            return AdHocAPI.getDatabaseList(param);
        },
        {
            refreshDeps: [location.pathname],
        },
    );
    const cols = getTableInfoRequest?.data?.data?.[tbl_name]?.schema;
    useEffect(() => {
        if (cols) {
            setColumns(getColumns(cols[0], props, false))
        } else {
            setColumns([])
        }
    }, [cols])
    
    const history = useHistory();
    function goImport(){
        if(db_name && tbl_name){
            history.push('/Playground/import/'+db_name+'-'+tbl_name);
        } else {
            notification.error({message: t('selectWarning')});
        }
    }
    return (
        <Tabs
            // activeKey={TabPaneType.Structure}
            onChange={key => {if (key ===QUERY_TABTYPE.key3) {goImport()}}}
        >
            <Tabs.TabPane tab={t('tableStructure')} key={QUERY_TABTYPE.key1}>
                <Row justify="space-between" style={{marginBottom: 10}}>
                    <span>
                    {t('database')}: {db_name} &nbsp;&nbsp;&nbsp;&nbsp; {t('table')}: {tbl_name}
                    </span>
                    <FlatBtn
                        onClick={() =>
                            props.queryTable(`${db_name}.${tbl_name}`)
                        }
                    >
                        {t('queryForm')}
                    </FlatBtn>
                </Row>
                <Table
                    bordered
                    rowKey='Field'
                    columns={columns}
                    scroll={{ y: '36vh' }}
                    loading={{
                        spinning: getTableInfoRequest.loading,
                        delay: TABLE_DELAY,
                    }}
                    dataSource={cols}
                    size="small"
                />
            </Tabs.TabPane>
            <Tabs.TabPane
                tab={t('dataPreview')}
                key={QUERY_TABTYPE.key2}
            >
                <DataPrev></DataPrev>
            </Tabs.TabPane>
            <Tabs.TabPane
                tab={t('dataImport')} key={QUERY_TABTYPE.key3}
            >
                {/* <DataImport/> */}
            </Tabs.TabPane>
        </Tabs>
    );
}
 
