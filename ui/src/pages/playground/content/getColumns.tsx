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

export default function getColumns(data, fa, isAction){
    if(!data){
        return []
    }
    let arr = []
    for(let i in data){
        if(i === 'key'){
            continue
        }
        if ( i === 'Field' && fa && !isAction ){
            arr.push({
                title: 'Field',
                dataIndex: 'Field',
                key: 'Field',
                render: Field => (
                    <span
                        style={{cursor: 'pointer'}}
                        onClick={() => fa.handleNameClicked(Field)}
                    >
                        {Field}
                    </span>
                ),
            })
        } else {
            arr.push({
                title: i,
                dataIndex: i,
                key: i,
                render: i => (
                    <span>
                        {i === '\t'?'\\t':i}
                    </span>
                ),
            })
        }
    }
    if(isAction){
        arr.push(
            {
                title: 'Action',
                dataIndex: '',
                key: 'x',
                render: (text, record, index) => {
                    return <a onClick={(e) => {fa(record, index, e)}}>Delete</a>
                },
            },
        )
    }
    return arr;
}
 
