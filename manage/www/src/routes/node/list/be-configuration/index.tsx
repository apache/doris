/* eslint-disable prettier/prettier */
import React, { useEffect, useState } from 'react';
import { Descriptions, Radio, Select, Table, Space, Modal, Form, Input, Tooltip, message, Divider } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
const { Option } = Select;
import { DEFAULT_NAMESPACE_ID } from '@src/config';
import { ConfigurationTypeEnum } from '@src/common/common.data';
import { BeConfigAPI } from './be-config.api';
import { useHistory } from 'react-router-dom';
import { FILTERS} from '../config.data'
function Configuration() {
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
    const [nodeValue, setNodeValue] = useState<any>([]);
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
                text: '只选当前页',
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
            history.push(`/${DEFAULT_NAMESPACE_ID}/be-configuration`);
        } else {
            localStorage.setItem("nodeText","");
            history.push(`/${DEFAULT_NAMESPACE_ID}/fe-configuration`);
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
                        message.success('修改成功！');
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
                        message.success('修改成功！');
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
            setNodeValue(nodeText)
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
            message.warning('请勾选要修改的配置！');
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
                            title: '操作',
                            dataIndex: item,
                            render: (text: any, record: any, index: any) => (
                                <Space size="middle">
                                    {record[Object.keys(record).length - 2] === 'true' ? (
                                        <a onClick={() => changeConfig(record)}>编辑</a>
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
            setNodeValue([])
            
        } else {
            setNodeValue(value) 
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
    const [configValue, setConfigValue] = React.useState<any>([]);
    const configSelectProps = {
        mode: 'multiple' as const,
        style: { width: '100%' },
        configValue,
        onChange: (newValue: string[]) => {
            setConfigValue(newValue);
        },
        maxTagCount: 'responsive' as const,
    };
    const nodeSelectProps = {
        mode: 'multiple' as const,
        style: { width: '100%' },
        nodeValue,
        onChange: (newValue: string[]) => {
            setNodeValue(newValue);
        },
        maxTagCount: 'responsive' as const,
    };
    return (
        <div>
            <Descriptions style={{ marginTop: 20 }}>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>节点选择：</span>
                    <Radio.Group onChange={onChange} value={typeValue} style={{ marginTop: '4px' }}>
                        <Radio value={ConfigurationTypeEnum.FE}>FE节点</Radio>
                        <Radio value={ConfigurationTypeEnum.BE}>BE节点</Radio>
                    </Radio.Group>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>配置项：</span>
                    <Select
                        {...configSelectProps}
                        mode="tags"
                        allowClear
                        placeholder="搜索配置项"
                        onChange={configSelectChange}
                        style={{ width: 250 }}
                        
                    >
                        {configSelect}
                    </Select>
                </Descriptions.Item>
                <Descriptions.Item style={{ paddingLeft: 40 }}>
                    <span>节点：</span>
                    <Select
                        {...nodeSelectProps}
                        mode="tags"
                        allowClear
                        placeholder="搜索节点"
                        onChange={nodeSelectChange}
                        value={nodeValue}
                        style={{ width: 250 }}
                    >
                        {nodeSelect}
                    </Select>
                </Descriptions.Item>
            </Descriptions>
            <Descriptions style={{ margin: '0 0 0 3em' }}>
            <Descriptions.Item >
                <span >
                        当前选中 {selectedRowKeys.length} 条数据
                    </span>
                </Descriptions.Item>
                
                <Descriptions.Item > 
                    <a onClick={batchEditing} style={{marginLeft: '86%' }} >
                        批量编辑
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
                        showTotal: (total: any) => `共 ${total} 条`,
                    }}
                ></Table>
            </div>
            <Modal
                title="编辑配置项"
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
                    <Form.Item label="配置值: " name="value" rules={[{ required: true, message: '配置值不能为空' }]}>
                        <Input placeholder="请输入配置值" />
                    </Form.Item>
                    <Form.Item
                        name="persist"
                        label="生效状态: "
                        rules={[{ required: true, message: '请选择生效状态' }]}
                    >
                        <Radio.Group value={'false'}>
                            <Radio value="false">
                                暂时生效
                                <Tooltip title="暂时生效，指只在当前生效，节点重启后不再生效，恢复修改之前的配置值">
                                    <InfoCircleOutlined style={{ marginLeft: '0.2em' }} />
                                </Tooltip>
                            </Radio>
                            <Radio value="true">
                                永久生效
                                <Tooltip title="永久生效，指修改后永久生效">
                                    <InfoCircleOutlined style={{ marginLeft: '0.2em' }} />
                                </Tooltip>
                            </Radio>
                        </Radio.Group>
                    </Form.Item>
                </Form>
            </Modal>
        </div>
    );
}

export default Configuration;
function warning(failed: []) {
    const arr = failed.map((failedMsg:any) => {
        return <><span style = {{color:'red'}}>{failedMsg.node}</span><span>节点的</span><span style = {{color:'red'}}>{failedMsg.config_name}</span><span>出错: {failedMsg.err_info}</span><br></br></>
    })
    Modal.error({
        content: <>
            <Divider />
            <h4>以下配置值修改失败，请检查编辑内容和对应节点状态</h4>
            <div style={{ height: '150px', width: '100%', overflow: 'auto', }}>
                {arr}
            </div>
        </>
        ,
        title: <span style ={{color:'red'}}>修改失败</span>,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        onOk() { },
        bodyStyle: { height: '250px' },
        width: '520px',
        closable:true

  });
}