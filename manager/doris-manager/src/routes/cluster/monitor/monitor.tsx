// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/* eslint-disable prettier/prettier */
import { Row, Col, Select, Collapse, Card, Button, Divider } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import dayjs from 'dayjs';
import './monitor.less';

import { LineChart } from 'echarts/charts';
import {
    GridComponent,
    PolarComponent,
    RadarComponent,
    GeoComponent,
    SingleAxisComponent,
    ParallelComponent,
    CalendarComponent,
    GraphicComponent,
    ToolboxComponent,
    TooltipComponent,
    TitleComponent,
    LegendComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { MonitorAPI } from './monitor.api';
import { CHARTS_OPTIONS, getTimes } from './monitor.data';
import { deepClone, formatBytes } from '@src/utils/utils';
import classNames from 'classnames';

echarts.use([
    LineChart,
    CanvasRenderer,
    TitleComponent,
    GridComponent,
    PolarComponent,
    RadarComponent,
    GeoComponent,
    SingleAxisComponent,
    ParallelComponent,
    CalendarComponent,
    GraphicComponent,
    ToolboxComponent,
    TooltipComponent,
    LegendComponent,
]);

const { Panel } = Collapse;

const gridStyle: React.CSSProperties = {
    width: 'calc(100%  / 6)',
    textAlign: 'center',
};
export function ClusterMonitor() {
    const { t } = useTranslation();
    const [TIMES, setTIMES] = useState(() => getTimes(dayjs()));
    const [statisticInfo, setStatisticInfo] = useState<any>({});
    const [disksCapacity, setDisksCapacity] = useState<any>({});
    // const [feNodes, setFENodes] = useState<string[]>([]);
    const [statistic, setStatistic] = useState<any>([]);

    const [currentTime, setCurrentTime] = useState(TIMES[1]);
    const [qps, setQPS] = useState({});
    const [errorRate, setErrorRate] = useState({});
    const [queryLatency, setQueryLatency] = useState({});
    const [connectionTotal, setConnectionTotal] = useState({});
    const [txnStatus, setTxnStatus] = useState({});
    const [pointNum] = useState('100');
    const [scheduled, setScheduled] = useState({});

    useEffect(() => {
        init();
    }, [currentTime.value, currentTime.start]);

    function init() {
        getStatisticInfo();
        getDisksCapacity();
        getStatistic();

        getQPS();
        getQueryLatency();
        getErrorRate();
        getConnectionTotal();
        getTxnStatus();
        getScheduled();
    }

    function getStatistic() {
        MonitorAPI.getStatistic()
            .then(res => {
                setStatistic(res.data);
            })
            .catch(err => {
                console.log(err);
            });
    }

    function getScheduled() {
        MonitorAPI.getScheduled({
            start: currentTime.start,
            end: currentTime.end,
            point_num: pointNum,
        })
            .then(res => {
                const option = formatData(res.data, '{t`clusterScheduling`}');
                option.legend.left = 20
                setScheduled(option);
            })
            .catch(err => {
                console.log(err);
            });
    }

    function getStatisticInfo() {
        MonitorAPI.getStatisticInfo()
            .then(res => {
                setStatisticInfo(res.data);
            })
            .catch(err => {
                console.log(err);
            });
    }
    function getDisksCapacity() {
        MonitorAPI.getDisksCapacity()
            .then(res => {
                setDisksCapacity(res.data);
            })
            .catch(err => {
                console.log(err);
            });
    }

    function formatData(response: any, title: string) {
        const option = deepClone(CHARTS_OPTIONS);
        // option.title.text = title;
        const xAxisData = response.x_value.map((item: string) => dayjs(item).format(currentTime.format));
        option.xAxis.data = xAxisData;
        const series: any[] = [];
        const legendData: any[] = [];
        function transformChartsData(y_value: string, formatter?: string) {
            for (const [key, value] of Object.entries(y_value)) {
                if (Array.isArray(value)) {
                    legendData.push(key);
                    series.push({
                        name: key,
                        data: value.map(item => {
                            if (formatter === 'MB') {
                                return typeof item === 'number' ? formatBytes(item, 2, false) : item;
                            }
                            return typeof item === 'number' ? item.toFixed(3) : item;
                        }),
                        type: 'line',
                    });
                } else {
                    transformChartsData(value);
                }
            }
            return series;
        }
        if (title === '节点内存(MB)') {
            transformChartsData(response.y_value, 'MB');
        } else {
            transformChartsData(response.y_value);
        }
        
        option.legend.data = legendData;
        option.grid = {
            bottom: 110,
        };
        option.series = series;
        return option;
    }
    function getQPS() {
        MonitorAPI.getQPS({
            type: 'qps',
            start: currentTime.start,
            end: currentTime.end,
        })
            .then(res => {
                const option = formatData(res.data,t`QPS`);
                setQPS(option);
            })
            .catch(err => {
                console.log(err);
            });
    }
    function getErrorRate() {
        MonitorAPI.getErrorRate({
            start: currentTime.start,
            end: currentTime.end,
        })
            .then(res => {
                const option = formatData(res.data, t`queryErrorRate`);
                setErrorRate(option);
            })
            .catch(err => {
                console.log(err);
            });
    }
    function getQueryLatency() {
        MonitorAPI.getQueryLatency({
            start: currentTime.start,
            end: currentTime.end,
            quantile: '0.99',
        })
            .then(res => {
                const option = formatData(res.data, t`99th`);
                option.grid.left = 60
                option.legend.left = 40
                setQueryLatency(option);
            })
            .catch(err => {
                console.log(err);
            });
    }
    function getConnectionTotal() {
        MonitorAPI.getConnectionTotal({
            start: currentTime.start,
            end: currentTime.end,
        })
            .then(res => {
                const option = formatData(res.data, t`numberOfConnections`);
                option.legend.left = 20
                option.grid = {
                    bottom: 120,
                };
                setConnectionTotal(option);
            })
            .catch(err => {
                console.log(err);
            });
    }
    function getTxnStatus() {
        MonitorAPI.getTxnStatus({
            start: currentTime.start,
            end: currentTime.end,
        })
            .then(res => {
                const option = formatData(res.data, t`importRate`);
                function transformToZh_CN(text: string) {
                    switch (text) {
                        case 'success':
                            return '导入成功';
                        case 'failed':
                            return '导入失败';
                        case 'begin':
                            return '提交导入';
                    }
                }
                const series = option.series.map((item: any) => {
                    item.name = transformToZh_CN(item.name);
                    return item;
                });
                const legendData = option.legend.data.map((item: any) => {
                    return transformToZh_CN(item);
                });
                option.series = series;
                option.legend.data = legendData;
                option.legend.left = 20
                option.grid = {
                    bottom: 120,
                };
                option.legend.bottom = 25
                option.color = ['#91cc75','#d9363e','#fac858']
                const txnStatus = option;
                setTxnStatus(txnStatus);
            })
            .catch(err => {
                console.log(err);
            });
    }

    return (
        <div style={{ background: '#fafafa' }}>
            <Row justify="space-between" align="middle">
                <Col></Col>
                <Row justify="start" align="middle" style={{ marginTop: '4px' }}>
                    <Col>{t`viewTime`}: </Col>
                    <Col style={{ margin: '0 10px' }}>
                        <Select
                            style={{ width: 200 }}
                            value={currentTime.value}
                            onChange={v => {
                                const time = TIMES.filter(item => item.value === v)[0];
                                setCurrentTime(time);
                            }}
                        >
                            {TIMES.map(time => {
                                return (
                                    <Select.Option key={time.value} value={time.value}>
                                        {time.text}
                                    </Select.Option>
                                );
                            })}
                        </Select>
                    </Col>
                    <Col>
                        <Button
                            type="primary"
                            onClick={() => {
                                const UPDATED_TIMES = getTimes(dayjs());
                                setTIMES(UPDATED_TIMES);
                                const time = UPDATED_TIMES.filter(item => item.value === currentTime.value)[0];
                                setCurrentTime(time);
                            }}
                        >
                            {t`refresh`}
                        </Button>
                    </Col>
                </Row>
            </Row>
            <div style={{ marginTop: 10 }}>
                <Collapse defaultActiveKey="1">
                    <Panel header={t`clusterInformation`} key="1">
                        <Card bordered={false}>
                            <Card.Grid style={gridStyle}>
                                <h4>{t`numberOfFeNodes`}</h4>
                                <span>{statisticInfo.fe_node_num_total}</span>
                            </Card.Grid>
                            <Card.Grid style={gridStyle}>
                                <h4>{t`FeNumberOfSurvivingNodes`}</h4>
                                <span>{statisticInfo.fe_node_num_alive}</span>
                            </Card.Grid>
                            <Card.Grid style={gridStyle}>
                                <h4>{t`NumberOfBENodes`}</h4>
                                <span>{statisticInfo.be_node_num_total}</span>
                            </Card.Grid>
                            <Card.Grid style={gridStyle}>
                                <h4>{t`BeNumberOfSurvivingNodes`}</h4>
                                <span>{statisticInfo.be_node_num_alive}</span>
                            </Card.Grid>
                            <Card.Grid style={gridStyle}>
                                <h4>{t`totalSpace`}</h4>
                                <span>{formatBytes(disksCapacity.be_disks_total) || t`noData`}</span>
                            </Card.Grid>
                            <Card.Grid style={gridStyle}>
                                <h4>{t`usedSpace`}</h4>
                                <span>{formatBytes(disksCapacity.be_disks_used) || t`noData`}</span>
                            </Card.Grid>
                        </Card>
                        <Row justify="start" >
                            <Row style={{ width: 'calc(100% / 3)' }}>
                                <h4>{t`QPS`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={qps}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 300, width: '100%',top:'-20px' }}
                                />
                            </Row>
                            <Row style={{ width: 'calc(100% / 3)' }}>
                                <h4>{t`99th`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={queryLatency}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 300, width: '100%',top:'-20px' }}
                                />
                            </Row>
                            <Row style={{ height: 300, width: 'calc(100% / 3)' }}>
                                <h4>{t`queryErrorRate`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={errorRate}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 300, width: '100%',top:'-20px' }}
                                />
                            </Row>
                        </Row>
                        <Divider />
                        <Row justify="start" >
                            <Row style={{ height: 300, width: 'calc(100% / 2)' }}>
                                <h4 style = {{marginLeft: '25px'}}>{t`numberOfConnections`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={connectionTotal}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 300, width: '100%',top:'-20px'  }}
                                />
                            </Row>
                            <Row style={{ height: 300, width: 'calc(100% / 2)' }}>
                                <h4 style = {{marginLeft: '25px'}}>{t`importRate`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={txnStatus}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 300, width: '100%',top:'-20px'  }}
                                />
                            </Row>
                        </Row>
                    </Panel>
                    <Panel header={t`dataMonitoring`} key="3" >
                        <Row justify="start" >
                            <Row style={{ height: 300, width: 'calc(100% / 2)' }}>
                                <h4>{t`clusterScheduling`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={scheduled}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 300, width: '100%' }}
                                />
                            </Row>
                            <Row style={{ height: 300, width: 'calc(100% / 2)' }}>
                                <Card.Grid style={{ textAlign: 'center', height: '33%', marginLeft: '12em' }}>
                                    <h4>{t`unhealthyFragmentation`}</h4>
                                    <span>{statistic.unhealthy_tablet_num}</span>
                                </Card.Grid>
                            </Row>
                        </Row>
                    </Panel>
                </Collapse>
            </div>
        </div>
    );
}
