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

import React, { useEffect, useRef, useState } from 'react';
import { Button, Col, Row, Typography, Space } from 'antd';
import { queryProfile } from 'Src/api/api';
import Table from 'Src/components/table';
import { useHistory } from 'react-router-dom';
import { Result } from '@src/interfaces/http.interface';
import { replaceToTxt } from 'Src/utils/utils';
import { useTranslation } from 'react-i18next';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { docco } from 'react-syntax-highlighter/dist/esm/styles/hljs';

const { Text, Title } = Typography;
export default function QueryProfile(params: any) {
    // const [parentUrl, setParentUrl] = useState('');
    const container = useRef<HTMLDivElement>(null);
    let { t } = useTranslation();
    const [allTableData, setAllTableData] = useState({
        column_names: [],
        rows: [],
    });
    const [profile, setProfile] = useState<any>();
    const history = useHistory();
    const doQueryProfile = function (ac?: AbortController) {
        const param = {
            path: getLastPath(),
            signal: ac?.signal,
        };
        queryProfile(param)
            .then((res: Result<any>) => {
                if (res && res.msg === 'success') {
                    if (!res.data.column_names) {
                        setProfile(res.data);
                        if (container.current !== null) {
                            container.current.innerHTML = res.data;
                        }
                    } else {
                        setProfile('');
                        res.data.column_names.push('Action');
                        res.data.rows = res.data.rows.map((row) => {
                            row['Sql Statement'] = (
                                <div style={{ maxWidth: 700 }}>
                                    <SyntaxHighlighter
                                        language="sql"
                                        style={docco}
                                    >
                                        {row['Sql Statement']}
                                    </SyntaxHighlighter>
                                </div>
                            );
                            row.Action = (
                                <Button
                                    size="small"
                                    onClick={() => {
                                        queryProfile<any>({
                                            path: row['Profile ID'],
                                        }).then((profileDetailRes) => {
                                            if (
                                                profileDetailRes &&
                                                profileDetailRes.msg ===
                                                    'success'
                                            ) {
                                                if (
                                                    !profileDetailRes.data
                                                        .column_names
                                                ) {
                                                    download(
                                                        profileDetailRes.data,
                                                        row['Profile ID']
                                                    );
                                                }
                                            }
                                        });
                                    }}
                                >
                                    {t('Download')}
                                </Button>
                            );
                            return row;
                        });
                        setAllTableData(res.data);
                    }
                } else {
                    setAllTableData({
                        column_names: [],
                        rows: [],
                    });
                }
            })
            .catch((err) => {});
    };
    useEffect(() => {
        const ac = new AbortController();
        doQueryProfile(ac);
        return () => ac.abort();
    }, [location.pathname]);

    function getLastPath() {
        let arr = location.pathname.split('/');
        let str = arr.pop();
        return str === 'QueryProfile' ? '' : str;
    }

    function goPrev() {
        if (location.pathname === '/QueryProfile/') {
            return;
        }
        history.push('/QueryProfile/');
    }

    function download(profile: string, profileId: string) {
        const profileTxt = replaceToTxt(profile);
        const blob = new Blob([profileTxt], {
            type: 'text/plain',
        });
        const tagA = document.createElement('a');
        tagA.download = `profile_${profileId}.txt`;
        tagA.style.display = 'none';
        tagA.href = URL.createObjectURL(blob);
        document.body.appendChild(tagA);
        tagA.click();
        URL.revokeObjectURL(tagA.href);
        document.body.removeChild(tagA);
    }
    function copyToClipboard(profile: string) {
        const profileTxt = replaceToTxt(profile);
        const textarea = document.createElement('textarea');
        textarea.value = profileTxt;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);
    }

    return (
        <Typography style={{ padding: '30px' }}>
            <Title>Finished Queries</Title>

            <Row style={{ paddingBottom: '15px' }}>
                <Col span={12}>
                    <Text strong={true}>
                        This table lists the latest 100 queries
                    </Text>
                </Col>
                <Col span={12} style={{ textAlign: 'right' }}>
                    {profile ? (
                        <Space>
                            <Button type="primary" onClick={goPrev}>
                                Back
                            </Button>
                            <Button
                                onClick={() => {
                                    const path = getLastPath();
                                    download(profile, path as string);
                                }}
                            >
                                Download
                            </Button>
                            <Button
                                onClick={() => {
                                    copyToClipboard(profile);
                                }}
                            >
                                Copy Profile
                            </Button>
                        </Space>
                    ) : (
                        ''
                    )}
                </Col>
            </Row>
            {profile ? (
                <div
                    ref={container}
                    style={{ background: '#f9f9f9', padding: '20px' }}
                >
                    {/* {profile} */}
                </div>
            ) : (
                <Table
                    rowKey={(record) => record['Profile ID']}
                    isSort={true}
                    isFilter={true}
                    isInner={'/QueryProfile'}
                    allTableData={allTableData}
                />
            )}
        </Typography>
    );
}
