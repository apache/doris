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
 
import React, {useState, useEffect, useContext, SyntheticEvent} from 'react';
// import {Controlled as CodeMirror} from 'react-codemirror2';
import styles from './index.less';
import {
    CODEMIRROR_OPTIONS,
    AdhocContentRouteKeyEnum,
} from '../adhoc.data';
import {Button, Row, Col, notification} from 'antd';
import {PlayCircleFilled} from '@ant-design/icons';
import {Switch, Route, Redirect} from 'react-router';
import {AdhocContentResult} from './content-result';
import {useRequest} from '@umijs/hooks';
import {AdHocAPI} from 'Src/api/api';
import {Result} from '@src/interfaces/http.interface';
import {isSuccess, getDbName, getTimeNow} from 'Utils/utils';
import {CodeMirrorWithFullscreen} from 'Components/codemirror-with-fullscreen/codemirror-with-fullscreen';
import {
    openInEditorSubject,
    runSQLSuccessSubject,
} from '../adhoc.subject';
import {FlatBtn} from 'Components/flatbtn';
import {ContentStructure} from './content-structure';
import sqlFormatter from 'sql-formatter';
import {useTranslation} from 'react-i18next';
import {ResizableBox} from 'react-resizable';
require('react-resizable/css/styles.css');
let editorInstance: any;
let isQueryTableClicked = false;
let isFieldNameInserted = false;
export function AdHocContent(props: any) {
    let { t } = useTranslation();
    const {match} = props;
    const [loading, setLoading] = useState({
        favoriteConfirm: false,
    });
    const [code, setCode] = useState('');
    const editorAreaHeight = +(localStorage.getItem('editorAreaHeight') || 300);
    // const beginTime = getTimeNow();
    const runQuery = useRequest<Result<any>>(
        () =>
            AdHocAPI.doQuery({
                db_name:getDbName().db_name,
                body:{stmt:code},
            }),
        {
            manual: true,
            onSuccess: res => {
                // const endTime = getTimeNow();
                const {db_name, tbl_name} = getDbName();
                if (isSuccess(res)) {
                    res.sqlCode = code;
                    res = {...res, db_name, tbl_name}
                    props.history.push({pathname:`/Playground/result/${db_name}-${tbl_name}`,state: res});
                    runSQLSuccessSubject.next(true);
                } else {
                    res.sqlCode = code;
                    res = {...res, db_name, tbl_name}
                    props.history.push({pathname:`/Playground/result/${db_name}-${tbl_name}`,state: res});
                    runSQLSuccessSubject.next(false);
                }
            },
            onError: () => {
                notification.error({message: t('errMsg')});
                runSQLSuccessSubject.next(false);
            },
        },
    );

    useEffect(() => {
        const subscription = openInEditorSubject.subscribe(code => {
            setCode(code);
            setEditorCursor(false);
        });
        return () => subscription.unsubscribe();
    }, []);

    const handleChange = (_editor, _data, value) => {
        setCode(value);
    };


    function handleQueryTable(tableName: string) {
        isQueryTableClicked = true;
        setCode(`SELECT  FROM ${tableName}`);
        setEditorCursor(false);
    }

    function setEditorCursor(insert = false) {
        setTimeout(() => {
            editorInstance.focus();
            editorInstance.setCursor({line: 0, ch: 7});
            isFieldNameInserted = insert;
        }, 0);
    }


    function handleRunQuery() {
        runQuery.run();
    }

    function handleNameClicked(fieldName: string) {
        const currentPos = editorInstance.getCursor();
        const insertFieldName = isFieldNameInserted
            ? `, ${fieldName}`
            : `${fieldName}`;
        editorInstance.replaceRange(insertFieldName, currentPos);
        setTimeout(() => {
            editorInstance.focus();
            isFieldNameInserted = true;
        }, 0);
    }
    return (
        <div>
            <Row justify="space-between" style={{marginBottom: 10}}>
                <Col>{t('editor')} &nbsp;&nbsp;
                    <Button size='small' onClick={() => {
                        const sqlStr = sqlFormatter.format(code);
                        setCode(sqlStr);
                    }}>{t('format')}</Button>
                </Col>
                <Col><FlatBtn onClick={() => {
                    setCode('');
                    editorInstance.focus();
                }}>{t('clear')}</FlatBtn></Col>
            </Row>
            <Row justify="space-between" style={{marginBottom: 10, fontSize:'12px'}}>
                {t('currentDatabase')}: {getDbName().db_name}
            </Row>
            <ResizableBox
                width={Infinity}
                height={editorAreaHeight}
                onResizeStop={(e: SyntheticEvent, data: any) => {
                    const height = data.size.height || 300;
                    localStorage.setItem('editorAreaHeight', height);
                }}
                minConstraints={[Infinity, 125]}
                maxConstraints={[Infinity, 400]}
                axis="y"
            >
                <div className={styles['adhoc-code']}>
                    <CodeMirrorWithFullscreen
                        value={code}
                        options={CODEMIRROR_OPTIONS}
                        onBeforeChange={handleChange}
                        editorDidMount={editor => (editorInstance = editor)}
                        // onFocus={() => isFieldNameInserted = false}
                        onCursor={(editor: any, data: any) => {
                            if (data.xRel || isQueryTableClicked) {
                                isFieldNameInserted = false;
                            }
                        }}
                        isDoris={props.isDoris}
                    />
                    <Row
                        justify="space-between"
                        className={styles['adhoc-code-operator']}
                    >
                        <Col>
                            <Button
                                icon={<PlayCircleFilled/>}
                                disabled={!code}
                                type="primary"
                                onClick={() => handleRunQuery()}
                                loading={runQuery.loading}
                            >
                                {t('execute')}
                            </Button>
                        </Col>
                        {/* <Button
                            onClick={() =>props.isDoris?setShowHistory(true):props.history.push('/adhoc/history')}
                            type="default"
                        >
                            历史查询
                        </Button> */}
                    </Row>
                </div>
            </ResizableBox>
            <Switch>
                <Route
                    path={`${match.path}/${AdhocContentRouteKeyEnum.Result}/:jobId`}
                    render={props => (
                        <AdhocContentResult
                            {...props}
                        />
                    )}
                />
                <Route
                    path={`${match.path}/${AdhocContentRouteKeyEnum.Structure}/:table`}
                    render={props => (
                        <div style={{display:'flex',height:'53vh'}}>
                            <div style={{flex:3}}>
                                <ContentStructure
                                    queryTable={tableName =>
                                        handleQueryTable(tableName)
                                    }
                                    handleNameClicked={fieldName =>
                                        handleNameClicked(fieldName)
                                    }
                                    isDoris={props.isDoris}
                                    // tabChange={() =>console.log(11)}
                                />
                            </div>
                            {/* <div style={{flex:1}}>
                                <HistoryQuery>
                                </HistoryQuery>
                            </div> */}
                        </div>
                    )}
                />
                {/* <Route
                    path={`${match.path}/${AdhocContentRouteKeyEnum.Default}`}
                    component={() => <></>}
                /> */}
                {/* <Redirect
                    to={`${match.path}/${AdhocContentRouteKeyEnum.Default}`}
                /> */}
            </Switch>
        </div>
    );
}
 
