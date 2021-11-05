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
import { Descriptions, Radio, Select, Table, Space, Modal, Form, Input, Tooltip, message, Divider } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
const { Option } = Select;
import { ConfigurationTypeEnum } from '@src/common/common.data';
import { BeConfigAPI } from './be-config.api';
import { useHistory } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { FILTERS } from '../config.data'

function Configuration() {
    const { t } = useTranslation();
    const history = useHistory();
    const [beColumn, setBeColumn] = useState<any>([]);
    const [tableData, setTableData] = useState<any>([]);
    const [configSelect, setConfigSelect] = useState<any>([]);
    const [nodeSelect, setNodeSelect] = useState<any>([]);
    const [confName, setConfName] = useState<any>([]);
    const [nodeName, setNodeName] = useState<any>([]);
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [selectedRowKeys, setSelectedRowKeys] = useState<any>([]);
    const [selectedRows, setSelectedRows] = useState<any>([]);
    const [isBatchEdit, setIsBatchEdit] = useState<boolean>(true);
    const [rowData, setRowsData] = useState<any>({});
    const [changeCurrent, setChangeCurrent] = useState<number>();
    const [nodevalue, setNodevalue] = useState<any>([]);
    function changeConfig(record: any) {
        setRowsData(record);
        setIsBatchEdit(true);
        setIsModalVisible(true);
    }

    const [form] = Form.useForm();

    const rowSelection = {
        selectedRowKeys,
        onChange: (selectedRowKeys: React.Key[], selectedRows: any) => {
            console.log(selectedRows)
            let newSelectedRowKeys = [];
            let newSelectedRows: any[] = [];
            newSelectedRowKeys = selectedRowKeys.filter((key, index) => {
                if (selectedRows[index][Object.keys(selectedRows[index]).length - 2] === 'true') {
                    newSelectedRows.push(selectedRows[index]);
                    return true;
                }
                return false;
            });
            setSelectedRowKeys(newSelectedRowKeys);
            setSelectedRows(newSelectedRows);
        },
        getCheckboxProps: (record: any) => {
            const arr = Object.keys(record);
            return {
                disabled: record[arr.length - 2] === 'false',
                key: record[arr.length - 1],
            };
        },
        selections: [
            Table.SELECTION_ALL,
            Table.SELECTION_NONE,
            {
                text: t`SelectOnlyTheCurrentPage`,
                onSelect(selectedRowKeys: React.Key[], selectedRows: any) {
                    setSelectedRowKeys(selectedRowKeys)
                    let newSelectedRows: any[] = [];
                    for (let i = 0; i < selectedRowKeys.length; i++) {
                        newSelectedRows.push(tableData[selectedRowKeys[i]])
                    }
                    setSelectedRows(newSelectedRows)
                },
            },
        ],
    };
    const [typeValue, setTypeValue] = React.useState(ConfigurationTypeEnum.BE);
    function onChange(e: any) {
        setTypeValue(e.target.value);
        if (e.target.value === ConfigurationTypeEnum.BE) {
            localStorage.setItem("nodeText","");
            history.push(`/configuration/be`);
        } else {
            localStorage.setItem("nodeText","");
            history.push(`/configuration/fe`);
        }
    }
    const handleOk = (values: any) => {
        if (isBatchEdit) {
            let name = rowData[0];
            let data = {};
            data[name] = {
                node: [rowData[1]],
                value: values.value,
                persist: values.persist,
            };

            BeConfigAPI.setConfig(data).then(res => {
                if (res.code === 0) {
                    setIsModalVisible(false);
                    getTable()
                    if (res.data.failed && res.data.failed.length !== 0) {
                        warning(res.data.failed)
                    } else {
                        message.success(t`SuccessfullyModified`);
                    }
                } else {

                    message.error(res.msg);
                }
            });
        } else {
            let name = "";
            let data = {};
            for (let i = 0; i < selectedRows.length; i++) {
                let nodes: any[] = [];
                name = selectedRows[i][0]
                selectedRows.map((item: any[], index: any) => {
                    if (item[0] === selectedRows[i][0]) {
                        nodes.push(item[1])
                    }
                });
                data[name] = {
                    node: nodes,
                    value: values.value,
                    persist: values.persist,
                };
            }
            BeConfigAPI.setConfig(data).then(res => {
                if (res.code === 0) {
                    setIsModalVisible(false);
                    getTable()
                    if (res.data.failed && res.data.failed.length !== 0) {
                        warning(res.data.failed)
                    } else {
                        message.success(t`SuccessfullyModified`);
                    }
                } else {
                    message.error(res.msg);
                }
            });
        }
        console.log(values, isBatchEdit, rowData);
        //setIsModalVisible(false);
    };

    const handleCancel = () => {
        setIsModalVisible(false);
    };

    function getTable() {
        let data = {};
        if (confName.length === 0 && nodeName.length !== 0) {
            data = { type: typeValue, node: nodeName };
        }
        if (confName.length !== 0 && nodeName.length === 0) {
            data = { type: typeValue, conf_name: confName };
        }
        if (confName.length === 0 && nodeName.length === 0) {
            data = { type: typeValue };
        }
        if (confName.length !== 0 && nodeName.length !== 0) {
            data = { type: typeValue, conf_name: confName, node: nodeName };
        }
        setTable(data,typeValue)
    }
    function refresh() {
        let data = {};
        const nodeText = localStorage.getItem("nodeText")
        if (nodeText !== "") {
            setNodevalue(nodeText)
            nodeName.push(nodeText)
            const node = [];
            node.push(nodeText)
            if (confName.length === 0 && node.length !== 0) {
                data = { type: typeValue, node: node };
            }
            if (confName.length !== 0 && node.length === 0) {
                data = { type: typeValue, conf_name: confName };
            }
            if (confName.length === 0 && node.length === 0) {
                data = { type: typeValue };
            }
            if (confName.length !== 0 && node.length !== 0) {
                data = { type: typeValue, conf_name: confName, node: node };
            }
        } else {
            if (confName.length === 0 && nodeName.length !== 0) {
                data = { type: typeValue, node: nodeName };
            }
            if (confName.length !== 0 && nodeName.length === 0) {
                data = { type: typeValue, conf_name: confName };
            }
            if (confName.length === 0 && nodeName.length === 0) {
                data = { type: typeValue };
            }
            if (confName.length !== 0 && nodeName.length !== 0) {
                data = { type: typeValue, conf_name: confName, node: nodeName };
            }
        }
        
        setTable(data,typeValue)

        BeConfigAPI.getConfigSelect().then((res: { data: any; code: any; msg: any }) => {
            if (res.code === 0) {
                const { data, code, msg } = res;
                console.log(data);
                const { backend, frontend } = data;
                const configSelect = backend.map((item: string, index: number) => {
                    return (
                        <Option key={item} value={item}>
                            {item}
                        </Option>
                    );
                });
                setConfigSelect(configSelect);
            } else {
                message.warning(res.msg);
            }
        });
        BeConfigAPI.getNodeSelect().then((res: { data: any; code: any; msg: any }) => {
            if (res.code === 0) {
                const { data, code, msg } = res;
                const { backend, frontend } = data;
                const nodeSelect = backend.map((item: string, index: number) => {
                    return (
                        <Option key={item} value={item}>
                            {item}
                        </Option>
                    );
                });
                setNodeSelect(nodeSelect);
            } else {
                message.warning(res.msg);
            }
        });
    }

    function batchEditing() {
        if (selectedRowKeys.length === 0) {
            message.warning(t`PleaseCheckTheConfigurationYouWantToModify`);
        } else {
            setIsModalVisible(true);
            setIsBatchEdit(false);
        }
    }
    useEffect(() => {
        refresh();
    }, []);
    function configSelectChange(_value: any) {
        setSelectedRowKeys([])
        setSelectedRows([])
        const newArray =  _value.map(x => x.trim())
        setConfName(newArray);
        let data = {};
        if (newArray.length === 0 && nodeName.length !== 0) {
            data = { type: typeValue, node: nodeName };
        }
        if (newArray.length !== 0 && nodeName.length === 0) {
            data = { type: typeValue, conf_name: newArray };
        }
        if (newArray.length === 0 && nodeName.length === 0) {
            data = { type: typeValue };
        }
        if (newArray.length !== 0 && nodeName.length !== 0) {
            data = { conf_name: newArray, node: nodeName };
        }
        setTable(data,typeValue)
    }

    function setTable(data: any,typeValue: any) {
        BeConfigAPI.getConfigurationsInfo(data, typeValue).then(res => {
            if (res.code == 0) {
                const { column_names, rows } = res.data;
                const columns = column_names.map((item: string, index: number) => {
                    if (item === '可修改') {
                        return {
                            title: t`operate`,
                            dataIndex: item,
                            render: (text: any, record: any, index: any) => (
                                <Space size="middle">
                                    {record[Object.keys(record).length - 2] === 'true' ? (
                                        <a onClick={() => changeConfig(record)}>{ t`edit`}</a>
                                    ) : (
                                        <></>
                                    )}
                                </Space>
                            ),
                            fixed: 'right',
                            width: 100,
                        };
                    }
                    if (item === 'MasterOnly') {
                        return {
                            title: item,
                            dataIndex: `${index}`,
                            key: item,
                            columnWidth: 10,
                            filters: FILTERS,
                            onFilter: (value: any, record: any) => onFilterCols(value,record),
                        };
                    }
                    else {
                        return {
                            title: item,
                            dataIndex: index,
                            columnWidth: 10,
                        };
                    }
                });
                setBeColumn(columns);

                for (let i = 0; i < rows.length; i++) {
                    rows[i].push('' + i + '');
                }
                setTableData(rows.map((item: string[]) => ({ ...item })));
                if (data.conf_name) {
                    setChangeCurrent(1)
                }
            } else {
                message.warning(res.msg);
            }
        });
    }

    function onFilterCols(value: any, record: any) {
        return record[6].indexOf(value) === 0
    }

    function nodeSelectChange(value: any) {
        setSelectedRowKeys([])
        setSelectedRows([])
        if (value.length === 0) {
            setNodevalue([])
            
        } else {
            setNodevalue(value) 
        }
        localStorage.setItem("nodeText", "");
        const newArray =  value.map(x => x.trim())
        setNodeName(newArray);
        let data = {};
        if (confName.length === 0 && newArray.length !== 0) {
            data = { type: typeValue, node: newArray };
        }
        if (confName.length !== 0 && newArray.length === 0) {
            data = { type: typeValue, conf_name: confName };
        }
        if (confName.length === 0 && newArray.length === 0) {
            data = { type: typeValue };
        }
        if (confName.length !== 0 && newArray.length !== 0) {
            data = { type: typeValue, conf_name: confName, node: newArray };
        }
        setTable(data,typeValue)
    }

    function pageSizeChange(current: React.SetStateAction<number | undefined>, pageSize: number | undefined) {
        setChangeCurrent(current)
    }
    const [configvalue, setConfigvalue] = React.useState<any>([]);
    const configSelectProps = {
        mode: 'multiple' as const,
        style: { width: '100%' },
        configvalue,
        onChange: (newValue: string[]) => {
            setConfigvalue(newValue);
        },
        maxTagCount: 'responsive' as const,
    };
    const nodeSelectProps = {
        mode: 'multiple' as const,
        style: { width: '100%' },
        nodevalue,
        onChange: (newValue: string[]) => {
            setNodevalue(newValue);
        },
        maxTagCount: 'responsive' as const,
    };
    return (
        <div>
            <Descriptions style={{ marginTop: 20 }}>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>{t`nodeSelection`}：</span>
                    <Radio.Group onChange={onChange} value={typeValue} style={{ marginTop: '4px' }}>
                        <Radio value={ConfigurationTypeEnum.FE}>FE节点</Radio>
                        <Radio value={ConfigurationTypeEnum.BE}>BE节点</Radio>
                    </Radio.Group>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>{ t`ConfigurationItem`}：</span>
                    <Select
                        {...configSelectProps}
                        mode="tags"
                        allowClear
                        placeholder={t`SearchConfigurationItems`}
                        onChange={configSelectChange}
                        style={{ width: 250 }}
                        
                    >
                        {configSelect}
                    </Select>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>{ t`Node`}：</span>
                    <Select
                        {...nodeSelectProps}
                        mode="tags"
                        allowClear
                        placeholder={ t`SearchNode`}
                        onChange={nodeSelectChange}
                        value={nodevalue}
                        style={{ width: 250 }}
                    >
                        {nodeSelect}
                    </Select>
                </Descriptions.Item>
            </Descriptions>
            <Descriptions style={{ margin: '0 0 0 3em' }}>
            <Descriptions.Item >
                <span >
                        { t`CurrentlySelected`} {selectedRowKeys.length} {t`StripData`}
                    </span>
                </Descriptions.Item>
                
                <Descriptions.Item > 
                    <a onClick={batchEditing} style={{marginLeft: '86%' }} >
                        {t`BatchEditing`}
                    </a>
                    </Descriptions.Item>
            </Descriptions>
            <div>
                <Table
                    columns={beColumn}
                    dataSource={tableData}
                    rowSelection={{
                        ...rowSelection
                    }}
                    bordered
                    rowKey={(record: any[]) => record[Object.keys(record).length - 1]}
                    size="middle"
                    style={{ paddingLeft: '40px', paddingRight: '40px' }}
                    scroll={{ x: 1300 }}
                    pagination={{
                        position: ['bottomCenter'],
                        total: beColumn,
                        current:changeCurrent,
                        showSizeChanger: true,
                        showQuickJumper: true,
                        onChange: (current, pageSize) => pageSizeChange(current, pageSize),
                        showTotal: (total: any) => {t`total`+`${total}` +t`strip`},
                    }}
                ></Table>
            </div>
            <Modal
                title={ t`EditConfigurationItems`}
                visible={isModalVisible}
                onCancel={handleCancel}
                onOk={() => {
                    form.validateFields()
                        .then((values: any) => {
                            form.resetFields();
                            handleOk(values);
                        })
                        .catch((info: any) => {
                            console.log('Validate Failed:', info);
                        });
                }}
            >
                <Form form={form} name="form_in_modal" initialValues={{ modifier: 'public' }}>
                    <Form.Item label={ t`ConfigurationValue`+":"} name="value" rules={[{ required: true, message: '配置值不能为空' }]}>
                        <Input placeholder={ t`PleaseEnterTheConfigurationValue`} />
                    </Form.Item>
                    <Form.Item
                        name="persist"
                        label={ t`EffectiveState`+":"}
                        rules={[{ required: true, message: t`PleaseSelectAnEffectiveState` }]}
                    >
                        <Radio.Group value={'false'}>
                            <Radio value="false">
                                {t`TemporarilyEffective`}
                                <Tooltip title={t`TemporarilyEffectiveTooltip`}>
                                    <InfoCircleOutlined style={{ marginLeft: '0.2em' }} />
                                </Tooltip>
                            </Radio>
                            <Radio value="true">
                                { t`Permanent`}
                                <Tooltip title={ t`PermanentTooltip`}>
                                    <InfoCircleOutlined style={{ marginLeft: '0.2em' }} />
                                </Tooltip>
                            </Radio>
                        </Radio.Group>
                    </Form.Item>
                </Form>
            </Modal>
        </div>
    );
    function warning(failed: []) {
        const arr = failed.map((failedMsg:any) => {
            return <><span style={{ color: 'red' }}>{failedMsg.node}</span><span>{ t`Node`}</span><span style = {{color:'red'}}>{failedMsg.config_name}</span><span>{t`Error`+":"} {failedMsg.err_info}</span><br></br></>
        })
        Modal.error({
            content: <>
                <Divider />
                <h4>{ t`ConfigurationError`}</h4>
                <div style={{ height: '150px', width: '100%', overflow: 'auto', }}>
                    {arr}
                </div>
            </>
            ,
            title: <span style={{ color: 'red' }}>{ t`FailToEdit`}</span>,
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            onOk() { },
            bodyStyle: { height: '250px' },
            width: '520px',
            closable:true
    
      });
    }
}

export default Configuration;
