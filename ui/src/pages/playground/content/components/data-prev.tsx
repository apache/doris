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
 
import React,{useState,useEffect} from 'react';
import {AdHocAPI} from 'Src/api/api';
import {getDbName} from 'Utils/utils';
import {Row, Empty} from 'antd';
import {FlatBtn} from 'Components/flatbtn';
import {useTranslation} from 'react-i18next';
export function DataPrev(props: any) {
    let { t } = useTranslation();
    const {db_name,tbl_name} = getDbName();
    const [tableData,setTableData] = useState([]);
    function toQuery(): void {
        AdHocAPI.doQuery({
            db_name,
            body:{stmt:`SELECT * FROM ${db_name}.${tbl_name} LIMIT 10`},
        }).then(res=>{
            if (res && res.msg === 'success') {
                setTableData(res.data);
            }
        })
        .catch(()=>{
            setTableData([]);
        });
    }
    useEffect(()=>{
        toQuery();
    },[location.pathname]);
    return (
        <div>
            <Row justify="space-between" style={{marginBottom: 10}}>
                <span style={{paddingBottom:'15px'}}>{t('dataPreview')}({t('display10')})</span>
                <span>
                    {db_name}.{tbl_name}
                </span>
                <FlatBtn
                    onClick={() =>
                        toQuery()
                    }
                >
                    {t('refresh')}
                </FlatBtn>
            </Row>
            <div
                className="ant-table ant-table-small ant-table-bordered"
                style={{marginTop: 10}}
            >
                {tableData?.meta?.length
                    ?<div className="ant-table-container" >
                        <div className='ant-table-content'>
                            <table style={{width: '100%'}}>
                                <thead className="ant-table-thead">
                                    <tr>
                                        {tableData?.meta?.map(item => (
                                            <th className="ant-table-cell" key={item.name}>
                                                {item.name}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody className="ant-table-tbody">
                                    {tableData.data?.map((item,index) => (
                                        <tr className="ant-table-row" key={index}>
                                            {item.map((tdData,index) => (
                                                <td
                                                    className="ant-table-cell"
                                                    key={tdData+index}
                                                >
                                                    {tdData == '\\N'?'-':tdData}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </div>:<Empty image={Empty.PRESENTED_IMAGE_SIMPLE}/>}
            </div>
        </div>
    );
}

 
