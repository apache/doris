import React, { useState } from 'react';
import { Descriptions, Radio, Select, Table, message } from 'antd';
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import { ConfigurationTypeEnum } from '@src/common/common.data';
import { useHistory } from 'react-router-dom';

function Configuration() {
    const history = useHistory();
    const [feColumn, setFeColumn] = useState<any>([]);

    const [tableData, setTableData] = useState<any>([]);

    const [configSelect, setConfigSelect] = useState<any>([]);

    const [nodeSelect, setNodeSelect] = useState<any>([]);

    const [selectedRowKeys, setSelectedRowKeys] = useState<any>([]);

    const rowSelection = {};

    function batchEditing() {
        if (selectedRowKeys.length === 0) {
            message.warning('请勾选要修改的配置！');
        }
    }
    function onChange(e: any) {
        if (e.target.value === ConfigurationTypeEnum.BE) {
            localStorage.setItem("nodeText","");
            history.push(`/${DEFAULT_NAMESPACE_ID}/be-configuration`);
        } else {
            localStorage.setItem("nodeText","");
            history.push(`/${DEFAULT_NAMESPACE_ID}/fe-configuration`);
        }
    }

    return (
        <>
            <Descriptions style={{ marginTop: 20 }}>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>节点选择：</span>
                    <Radio.Group onChange={onChange} style={{ marginTop: '4px' }}>
                        <Radio value={ConfigurationTypeEnum.FE}>FE节点</Radio>
                        <Radio value={ConfigurationTypeEnum.BE}>BE节点</Radio>
                    </Radio.Group>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>配置项：</span>
                    <Select mode="tags" allowClear placeholder="搜索配置项" style={{ width: 200 }}>
                        {configSelect}
                    </Select>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>节点：</span>
                    <Select mode="tags" allowClear placeholder="搜索节点" style={{ width: 200 }}>
                        {nodeSelect}
                    </Select>
                </Descriptions.Item>
                <Descriptions.Item>
                    {' '}
                    <a style={{ width: '100%', marginLeft: '90%' }} onClick={batchEditing}>
                        批量编辑
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
                        showTotal: (total: any) => `共 ${total} 条`,
                    }}
                ></Table>
            </div>
        </>
    );
}

export default Configuration;
