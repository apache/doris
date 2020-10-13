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
 
import React,{useState, useEffect, useLayoutEffect} from 'react';
import {getDbName} from 'Utils/utils';
import {Typography, Steps, Button, notification, Form, Input, Select, Upload, Table, Empty} from 'antd';
import {UploadOutlined} from '@ant-design/icons';
import {AdHocAPI, doUp, getUploadData, deleteUploadData} from 'Src/api/api';
const {Step} = Steps;
const {Option} = Select;
import {getAllTableData} from './import-func';
import {ImportResult} from './import-result';
import {useHistory} from 'react-router-dom';
import {useTranslation} from 'react-i18next';
import 'antd/lib/style/themes/default.less';
import getColumns from '../content/getColumns';
import './index.less';
import 'antd/dist/antd.css';
import {getBasePath} from 'Src/utils/utils';
export default function DataImport(props: any) {
    let { t } = useTranslation();
    let basePath = getBasePath();
    const history = useHistory();
    const [header, setHeader] = useState([])
    const [rowId, setRowId] = useState()
    const [cols, setCols] = useState([])
    const [headerUploadData, setHeaderUploadData] = useState([])
    const [colsUploadData, setColsUploadData] = useState([])
    const [prevData, setPrevData] = useState([]);
    const [prevBackData, setPrevBackData] = useState();
    const [labelV, setLabel] = useState();
    const [columnsV, setColumns] = useState();
    const [fileList, setFileList] = useState([]);
    const [column_separator, setColumnSeparator] = useState('\t');
    const {db_name, tbl_name}=getDbName();
    const [form] = Form.useForm();
    const steps = [
        {
            title: t('table'),
        },
        {
            title: t('selectTheFile'),
        },
        {
            title: t('loadConfig'),
        },
    ];
    const [current, setCurrent] = useState(0);
    const layout = {
        labelCol: {span: 3},
        wrapperCol: {span: 6},
        labelAlign: 'left',
    };
    const uploadData = {
        name: 'file',
        action: `${basePath}/api/default_cluster/${db_name}/${tbl_name}/upload`,
        data:{
            column_separator,
            preview:'true',
        },
        fileList,
        beforeUpload(file){
            if (file.size/1024/1024 > 100) {
                notification.error({message: t('fileSizeWarning')});
                return false;
            }
            return true
        },
        onChange(info) {
            // if (info.file.status !== 'uploading') {

            // }
            if (info.file.status === 'done') {
                notification.success({message: `${info.file.name} file uploaded successfully`});
                getUploadList()
            } else if (info.file.status === 'error') {
                notification.error({message: `${info.file.name} file upload failed.`});
                setHeaderUploadData([]);
                setColsUploadData([])
            }
            if (info.fileList.length === 2) {
                info.fileList = info.fileList.slice(-1);
                setFileList(info.fileList);
            } else if(info.fileList.length === 1) {
                setFileList(info.fileList);
            } 
            if(!info.file.status){
                setFileList([]);
            }
        },
        progress: {
            strokeColor: {
              '0%': '#108ee9',
              '100%': '#87d068',
            },
            strokeWidth: 3,
            format: percent => `${parseFloat(percent.toFixed(2))}%`,
        },
    };
    function next() {
        const num = current + 1;
        if (current === 1 && !prevBackData) {
            notification.error({message: t('uploadWarning')});
            return;
        }
        setCurrent( num );
    }

    function  prev() {
        const num = current - 1;
        setCurrent( num );
    }

    function getTableInfoRequest (){
        const param = {
            db_name,
            tbl_name,
        };
        AdHocAPI.getDatabaseList(param).then(
            res => {
                // const endTime = getTimeNow();
                const {db_name, tbl_name} = getDbName();
                if (res && res.msg === 'success') {
                    let cols = res.data[tbl_name]?.schema;
                    setCols(cols);
                    setHeader(getColumns(cols[0], false, false))
                } else {
                    setCols([])
                    setHeader([])
                }
            }
        ).catch(
            () => {
                notification.error({message:t('errMsg')});
            }
        )
    }

    function doUploadData() {
        form.validateFields()
            .then(value=>{
                const params = {
                    db_name,
                    tbl_name,
                    file_id: prevBackData.id,
                    file_uuid: prevBackData.uuid,
                    label: labelV,
                    columns: columnsV,
                    column_separator: prevBackData.columnSeparator
                };
                doUp(params).then(res=>{
                    if(res && res.msg === 'success'){
                        if(res.data){
                            notification.success({message: `${res.msg}`});
                            ImportResult(res.data,()=>{
                                history.push(`/Playground/structure/${db_name}-${tbl_name}`);
                            });
                        }
                    }
                });
            })
            .catch(errorInfo => {
                notification.error({message: `${errorInfo}`});
            });
    }
    useEffect(() => {
        getTableInfoRequest();
        getUploadList();
    }, []);
    function doBack(){
        history.push(`/Playground/structure/${db_name}-${tbl_name}`);
    }
    function getPrev(data){
        setRowId(data.id);
        getUploadData({
            db_name,
            tbl_name,
            file_id:data.id,
            file_uuid:data.uuid,
            preview:true
        }).then((res)=>{
            if (res.data && res.msg === 'success') {
                const data = res.data;
                setPrevBackData(data);
                setPrevData(getAllTableData(data.maxColNum , data.lines));
            } else {
                setPrevBackData('');
                setPrevData([]);
            }
        }).catch(err=>{
            setPrevBackData('');
            setPrevData([]);
        })
    }
    function getUploadList(){
        getUploadData({
            db_name,
            tbl_name,
        }).then((res)=>{
            if(res.data && res.msg === 'success'){
                let data = res.data;
                setHeaderUploadData(getColumns(data[0], deleteUpload, true));
                setColsUploadData(data)
            }
        }).catch(()=>{
            setHeaderUploadData([]);
            setColsUploadData([])
        })
    }
    function deleteUpload(data, i, e){
        e.stopPropagation();
        deleteUploadData({
            db_name,
            tbl_name,
            file_uuid:data.uuid,
            file_id:data.id
        }).then((res)=>{
            getUploadList();
            setPrevData([]);
            setPrevData([]);
            setPrevBackData('');
            setRowId('');
        })
    }
    function setRowClassName(record){
        return record.id === rowId ? 'clickRowStyle' : '';
    }
    return (
        <div style={{padding:'50px'}} >
            <Steps current={current}>
                {steps.map(item => (
                    <Step key={item.title} title={item.title}/>
                ))}
            </Steps>
            <div className="steps-content" style={{padding:'30px'}}>
                {/* {steps[current].content} */}
                {current === 0
                    ? <>
                        <Typography style={{'marginTop':'20px'}}>
                            <p>{t('database')}: {db_name}</p>
                            <p>{t('table')}: {tbl_name}</p>
                        </Typography>
                        <Typography style={{paddingBottom:'15px'}}>{t('tableStructure')}: </Typography>
                        <Table
                            bordered
                            rowKey='Field'
                            columns={header}
                            dataSource={cols}
                            size="small"
                        />
                    </>:''
                }
                {current === 1
                    ? <>
                        <Form
                            {...layout}
                        >
                            <Form.Item
                                label={t('delimiter')}
                                rules={[{required: true, message: t('delimiterWarning')}]}
                            >
                                <Select
                                    value={column_separator}
                                    onChange={value=>{
                                        setColumnSeparator(value);
                                    }}
                                    allowClear
                                >
                                    <Option value=",">,</Option>
                                    <Option value="|">|</Option>
                                    <Option value={'\t'}>\t</Option>
                                </Select>
                            </Form.Item>
                            <Form.Item
                                label={t('selectTheFile')}
                            >
                                <Upload {...uploadData}>
                                    <Button>
                                        <UploadOutlined/>{t('upload')}
                                    </Button>
                                </Upload>
                            </Form.Item>
                            <p>{t('uploadedFiles')}</p>
                            <Table
                                bordered
                                rowKey='uuid'
                                columns={headerUploadData}
                                dataSource={colsUploadData}
                                size="small"
                                pagination={false}
                                scroll={{ y: 240 }}
                                rowClassName={setRowClassName}
                                onRow={record => {
                                    return {
                                      onClick: event => {getPrev(record)}, 
                                    };
                                  }}
                            />
                            <p style={{marginTop:'20px'}}>{t('filePreview')}({t('display10')})</p>
                            <div
                                className="ant-table ant-table-small ant-table-bordered"
                                style={{marginTop: 10}}
                            >
                                <div className="ant-table-container" >
                                    <div className='ant-table-content'>
                                        <table style={{width: '100%'}}>
                                            <tbody className="ant-table-tbody">
                                                {prevData?.map((item,index) => (
                                                    <tr className="ant-table-row" key={index}>
                                                        {item.map((tdData,i) => (
                                                            <td
                                                                className="ant-table-cell"
                                                                key={i+tdData}
                                                            >
                                                                {tdData == '\\N'?'-':tdData}
                                                            </td>
                                                        ))}
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        </Form>
                    </>:''
                }
                {current === 2
                    ? <>
                        <Form form={form} {...layout}>
                            <Form.Item
                                label="Label"
                                name="Label"
                                rules={[{required: true, message: "Please Enter"}]}
                            >
                                <Input value={labelV} onChange={e=>setLabel(e.target.value)}/>
                            </Form.Item>
                            <Form.Item
                                label="columns"
                                name="columns"
                                rules={[{required: false, message: "Please Enter"}]}
                            >
                                <Input value={columnsV} onChange={e=>{setColumns(e.target.value);}}/>
                            </Form.Item>
                        </Form>
                    </>:''
                }
            </div>
            <div className="steps-action" style={{'textAlign':'end'}}>
                {current > 0 && (
                    <Button style={{margin: '0 8px'}} onClick={() => prev()}>
                        {t('previousStep')}
                    </Button>
                )}
                {current < steps.length - 1 && (
                    <Button type="primary" onClick={() => next()}>
                        {t('nextStep')}
                    </Button>
                )}
                {current === steps.length - 1 && (
                    <Button type="primary" onClick={() => doUploadData()}>
                        {t('loadButton')}
                    </Button>
                )}
                &nbsp;<Button onClick={() => doBack()}>
                    {t('cancelButton')}
                </Button>
            </div>
        </div>
    );
}
 
