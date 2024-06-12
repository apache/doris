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
import {
    Tabs,
    Row,
    Card,
    Col,
    Progress,
    Popover,
    Pagination,
} from 'antd';
import {TabPaneType} from '../adhoc.data';
import { useLocation } from 'react-router-dom'
import {FlatBtn} from 'Components/flatbtn';
import {
    DownloadOutlined,
    CloseCircleFilled,
} from '@ant-design/icons';
import SyntaxHighlighter from 'react-syntax-highlighter';
import {docco} from 'react-syntax-highlighter/dist/esm/styles/hljs';
import {TextWithIcon} from 'Components/text-with-icon/text-with-icon';
import {LoadingWrapper} from 'Components/loadingwrapper/loadingwrapper';
import {useTranslation} from 'react-i18next';

export function AdhocContentResult(props) {
    let { t } = useTranslation();
    let location = useLocation()
    const jobId = props.match.params.jobId;
    const [showSaveResultModal, setShowSaveResultModal] = useState(false);
    const [tableData, setTableDate] = useState([]);
    const [resStatus, setResStatus] = useState([]);
    // const [pageSize, setPageSize] = useState(20);
    const [total, setTotal] = useState([]);
    const [runningQueryInfo, setRunningQueryInfo] = useState<any>({});
    useEffect(() => {
        const runningQueryInfo = location.state;
        if (runningQueryInfo.msg=='success') {
            runningQueryInfo.status = 'success';
        } else {
            runningQueryInfo.status = 'failed';
        }
        if (runningQueryInfo.data?.type === 'exec_status') {
            setResStatus(runningQueryInfo.data.status)
        } else {
            const tableData = (runningQueryInfo.data && typeof(runningQueryInfo.data) === 'object')?(runningQueryInfo.data?.data).slice(0,20):[];
            setTableDate(tableData);
            setTotal((runningQueryInfo.data && typeof(runningQueryInfo.data) === 'object')?runningQueryInfo.data?.data.length:0);
        }
        setRunningQueryInfo(runningQueryInfo);
    },[location.state]);

    function renderRunStatus() {
        return (
            <>
                <Progress
                    style={{width: 340}}
                    percent={100}
                />
                <span style={{color: '#57C22D', marginLeft: 10}}>
                    {t('runSuccessfully')}
                </span>
            </>
        );
    }
    function getELe(data) {
        let arr = []
        for (const i in data) {
            arr.push(
                <Row justify="start" key={i}>
                    <Col span={3}>{i}:</Col>
                    <Col>{data[i]}</Col>
                </Row>
            );
        }
        return arr;
    }
    function changeShowTableItem(page,size){
        const allTableData =runningQueryInfo.data?.data;
        const tableData = allTableData.slice((page-1)*size,page*size);
        setTableDate(tableData);
    }
    return (
        <Tabs activeKey={TabPaneType.Result}>
            <Tabs.TabPane tab={t('results')} key={TabPaneType.Result}>
                <LoadingWrapper loading={false}>
                    {runningQueryInfo.status !== 'failed' ? (
                        <Row
                            style={{width: '100%', marginBottom: 10}}
                            justify="space-between"
                            align="middle"
                        >
                            <Row justify="start" align="middle">
                                {renderRunStatus()}
                            </Row>
                            {/* {runningQueryInfo.status === 'success' && (
                                <TextWithIcon
                                    onClick={() => setShowSaveResultModal(true)}
                                    style={{cursor: 'pointer'}}
                                    icon={<DownloadOutlined/>}
                                    isButton={true}
                                    text="保存结果"
                                />
                            )} */}
                        </Row>
                    ) : (
                        <TextWithIcon
                            icon={<CloseCircleFilled/>}
                            text={`${t('executionFailed')}: `+runningQueryInfo.msg +' '+ runningQueryInfo.data}
                            color="red"
                            style={{
                                marginBottom: 10,
                            }}
                        />
                    )}
                    <SyntaxHighlighter language="sql" style={docco}>
                        {runningQueryInfo.sqlCode?runningQueryInfo.sqlCode:''}
                    </SyntaxHighlighter>

                    <Card>
                        {/* <Row justify="start">
                            <Col span={3}>{t('queryForm')}:</Col>
                            <Col>{runningQueryInfo.tbl_name}</Col>
                        </Row> */}
                        <Row justify="start">
                            <Col span={3}>{t('executionTime')}:</Col>
                            <Col>{(runningQueryInfo.data?.time?runningQueryInfo.data?.time:0) + ' ms'}</Col>
                        </Row>
                        {/* <Row justify="start">
                            <Col span={3}>{t('endTime')}:</Col>
                            <Col>{runningQueryInfo.beginTime}</Col>
                        </Row> */}
                        {
                            ...getELe(resStatus)
                        }
                        {/* <Row>
                            <Col span={2}>结果表：</Col>
                            <FlatBtn onClick={() => queryResultTable()}>
                                临时表
                            </FlatBtn>
                        </Row> */}
                    </Card>
                    <div
                        className="ant-table ant-table-small ant-table-bordered"
                        style={{marginTop: 10, width: '100%', overflowX: 'scroll'}}
                    >
                        <div className="ant-table-container">
                            <div className='ant-table-content'>
                                <table style={{width: '100%'}}>
                                    <thead className="ant-table-thead">
                                        <tr>
                                            {runningQueryInfo.data?.meta?.map((item, index) => (
                                                <th className="ant-table-cell" key={index + item.name}>
                                                    {item.name}
                                                </th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody className="ant-table-tbody">
                                        {tableData.map((item,index) => (
                                            <tr className="ant-table-row" key={index}>
                                                {item.map((tdData, index) => (
                                                    <td
                                                        className="ant-table-cell"
                                                        key={index+''+tdData}
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
                    <Pagination
                        size="small"
                        total={total}
                        showSizeChanger
                        showQuickJumper
                        defaultCurrent={1}
                        defaultPageSize={20}
                        onChange={changeShowTableItem}
                        style={{'textAlign': 'right','marginTop': '10px'}}
                        showTotal={total => `Total ${total} items`}
                    />
                </LoadingWrapper>
            </Tabs.TabPane>
        </Tabs>
    );
}
 
