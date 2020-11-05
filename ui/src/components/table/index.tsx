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
import {Table, Popover, Input} from 'antd';
import {FilterFilled} from '@ant-design/icons';
import {getColumns, filterTableData} from './table.utils.tsx';
import './index.less';

export default function SortFilterTable(props: any) {
    const {isFilter=false, isSort=false, allTableData, isInner, isSystem=false, path=''} = props;
    const [tableData, setTableData] = useState([]);
    const [localColumns, setColumns] = useState([]);
    // function onChange(pagination, filters, sorter, extra) {
    //     console.log('params', pagination, filters, sorter, extra);
    // }
    function changeTableData(e){
        const localData = filterTableData(allTableData.rows,e.target.value);
        setTableData(localData);
    }
    const content = (
        <Input placeholder="Filter data" onChange={e=>changeTableData(e)}/>
    );
    useEffect(() => {
        if(allTableData.rows&&allTableData.column_names){
            setColumns(getColumns(allTableData.column_names, isSort, isInner, allTableData.href_columns||allTableData.href_column, path));
            setTableData(allTableData.rows);
        }
    }, [allTableData]);

    return(
        <span className='systemTable' >
            {isFilter?<Popover className={isSystem?'searchSystem':'search'} content={content} trigger="click">
                <FilterFilled/>
            </Popover>:''}
            <Table
                columns={localColumns}
                dataSource={tableData}
                scroll={{x:true}}
                size='small'
                bordered
                // onChange={onChange}
                pagination={{
                    size:'small',
                    showTotal:(total, range) => `${range[0]}-${range[1]} of ${total} items`,
                    showSizeChanger:true,
                    showQuickJumper:true,
                    hideOnSinglePage:true,
                }}
            />
        </span>
    );
}

 
