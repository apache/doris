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

import React, { useState } from 'react';
import { Descriptions, Radio, Select, Table, message } from 'antd';
import { ConfigurationTypeEnum } from '@src/common/common.data';
import { useHistory } from 'react-router-dom';
import { useTranslation } from 'react-i18next';

function Configuration() {
    const history = useHistory();
    const { t } = useTranslation();
    const [feColumn, setFeColumn] = useState<any>([]);

    const [tableData, setTableData] = useState<any>([]);

    const [configSelect, setConfigSelect] = useState<any>([]);

    const [nodeSelect, setNodeSelect] = useState<any>([]);

    const [selectedRowKeys, setSelectedRowKeys] = useState<any>([]);

    const rowSelection = {};

    function batchEditing() {
        if (selectedRowKeys.length === 0) {
            message.warning( t`PleaseCheckTheConfiguration`);
        }
    }
    function onChange(e: any) {
        if (e.target.value === ConfigurationTypeEnum.BE) {
            localStorage.setItem("nodeText","");
            history.push(`/configuration/be`);
        } else {
            localStorage.setItem("nodeText","");
            history.push(`/configuration/fe`);
        }
    }

    return (
        <>
            <Descriptions style={{ marginTop: 20 }}>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span> {t`nodeSelection`+"："}</span>
                    <Radio.Group onChange={onChange} style={{ marginTop: '4px' }}>
                        <Radio value={ConfigurationTypeEnum.FE}>FE{t`Node`}</Radio>
                        <Radio value={ConfigurationTypeEnum.BE}>BE{t`Node`}</Radio>
                    </Radio.Group>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>{ t`ConfigurationItem`}：</span>
                    <Select mode="tags" allowClear placeholder={ t`SearchConfigurationItems`} style={{ width: 200 }}>
                        {configSelect}
                    </Select>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>{ t`Node`}：</span>
                    <Select mode="tags" allowClear placeholder={ t`SearchNode`} style={{ width: 200 }}>
                        {nodeSelect}
                    </Select>
                </Descriptions.Item>
                <Descriptions.Item>
                    {' '}
                    <a style={{ width: '100%', marginLeft: '90%' }} onClick={batchEditing}>
                        { t`BatchEditing`}
                    </a>
                </Descriptions.Item>
            </Descriptions>
            <div>
                <Table
                    columns={feColumn}
                    dataSource={tableData}
                    rowSelection={{
                        ...rowSelection,
                    }}
                    bordered
                    rowKey={(record: any[]) => record[Object.keys(record).length - 1]}
                    size="middle"
                    style={{ paddingLeft: '40px', paddingRight: '40px' }}
                    scroll={{ x: 1300 }}
                    pagination={{
                        position: ['bottomCenter'],
                        total: tableData.length,
                        showSizeChanger: true,
                        showQuickJumper: true,
                        showTotal: (total: any) => {t`total`+`${total}` +t`strip`},
                    }}
                ></Table>
            </div>
        </>
    );
}

export default Configuration;
