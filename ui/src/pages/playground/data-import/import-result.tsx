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
 
import React from 'react';
import {Modal} from 'antd';
export function ImportResult(data,callback) {
    const arr = [];
    for (const i in data) {
        arr.push(
            <tr className="ant-table-row" key={i}>
                <td
                    className="ant-table-cell"
                    width='200'
                    key={i}
                >
                    {i}
                </td>
                <td
                    className="ant-table-cell"
                    key={data[i]}
                    width='200'
                >
                    {(data[i] && data[i].includes('http'))?<a href={data[i]} target="_blank">{data[i]}</a>:data[i]}
                </td>
            </tr>
        );
    }
    Modal.info({
        title: 'Import Result',
        width: 800,
        content: (
            <div
                className="ant-table ant-table-small ant-table-bordered"
                style={{marginTop: 15, marginLeft: '-38px'}}
            >
            <div className="ant-table-container" >
                <div className='ant-table-content'>
                    <table style={{width: '100%'}}>
                        <tbody className="ant-table-tbody">
                            {...arr}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        ),
        onOk() {
            callback();
        },
    });
}
 
