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

import React, {useState, useEffect} from 'react';
import {Typography, Divider, BackTop, Spin} from 'antd';
import {useTranslation} from "react-i18next";

const {Title, Paragraph} = Typography;
import {getHardwareInfo} from 'Src/api/api';

export default function Home(params: any) {
    const {t} = useTranslation();
    const [hardwareData, setHardwareData] = useState({});
    const [hardwareDataLoading, setHardwareDataLoading] = useState(true);
    const getConfigData = function () {
        getHardwareInfo().then(res => {
            if (res && res.msg === 'success') {
                setHardwareData(res.data);
                setHardwareDataLoading(false);
            }
        }).catch(err => {
            console.error(err)
            setHardwareData({VersionInfo: {}, HardwareInfo: {}});
        });
    };

    function getItems(data, flag) {
        const arr: any[] = [];
        for (const i in data) {
            let dt = data[i].replace(/&nbsp;/g, "")
            dt = dt.replace(/<br>/g, "\n")
            if (flag) {
                arr.push(<p key={i} dangerouslySetInnerHTML={createMarkup(i, dt)}/>)
            } else {
                arr.push(<p key={i}>{i + ' : ' + dt}</p>)
            }
        }
        return arr;
    }

    function createMarkup(key, data) {
        return {__html: key + ' : ' + String(data)};
    }

    useEffect(() => {
        getConfigData();
    }, []);
    return (
        <div>
            <Typography style={{padding: '30px'}}>
                <Title>Version</Title>
                <Spin spinning={hardwareDataLoading} tip={t('loading') + '...'}>
                    <Paragraph style={{background: '#f9f9f9', padding: '20px'}}>
                        {...getItems(hardwareData.VersionInfo, false)}
                    </Paragraph>
                </Spin>
                <Divider/>
                <Title>Hardware Info</Title>
                <Spin spinning={hardwareDataLoading} tip={t('loading') + '...'}>
                    <Paragraph style={{background: '#f9f9f9', padding: '20px'}}>
                        {...getItems(hardwareData.HardwareInfo, false)}
                    </Paragraph>
                </Spin>
            </Typography>
            <BackTop></BackTop>
        </div>
    );
}
  
 