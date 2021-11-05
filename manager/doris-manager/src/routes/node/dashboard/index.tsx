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

import { Row, Col, Select, Collapse, Button, Divider, AutoComplete } from 'antd';
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
const { Option } = Select;

export function NodeDashboard() {
    const { t } = useTranslation();
    const [TIMES, setTIMES] = useState(() => getTimes(dayjs()));
    const [beNodes, setBENodes] = useState<string[]>([]);
    const [selectedBENodes, setSelectedBENodes] = useState<string[]>([]);
    const [currentTime, setCurrentTime] = useState(TIMES[1]);
    const [be_cpu_idle, setBE_CPU_IDLE] = useState({});
    const [be_mem, setBE_Mem] = useState({});
    const [be_disk_io, setBE_DISK_IO] = useState({});
    const [be_base_compaction_score, setBE_base_compaction_score] = useState({});
    const [be_cumu_compaction_score, setBE_cumu_compaction_score] = useState({});

    useEffect(() => {
        init();
    }, [currentTime.value, currentTime.start]);

    useEffect(() => {
        updateBECharts();
    }, [selectedBENodes, currentTime.value, currentTime.start]);

    function handleBENodeChange(value: any) {
        setSelectedBENodes(value);
    }
    function init() {
        getBENodes();
    }

    function formatData(response: any, title: string) {
        console.log(response);
        const option = deepClone(CHARTS_OPTIONS);
        // option.title.text = title;
        const xAxisData = response.x_value.map((item: string) => dayjs(item).format(currentTime.format));
        option.xAxis.data = xAxisData;
        const series: any[] = [];
        const legendData: any[] = [];

        function transformChartsData(y_value: string, formatter?: string) {
            for (const [key, value] of Object.entries(y_value).sort()) {
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
        if (title === '节点内存使用量') {
            transformChartsData(response.y_value, 'MB');
        } else {
            transformChartsData(response.y_value);
        }
        option.legend = {
            bottom: '0',
            left: 0,
            height: 80,
            width: 'auto',
            type: 'scroll',
            itemHeight: 10,
            data: legendData,
            orient: 'vertical',
            pageIconSize: 7,
            textStyle: {
                overflow: 'breakAll',
            },
        };
        (option.grid = {
            bottom: 110,
        }),
            (option.series = series);
        return option;
    }

    // BE NODES REQUEST
    function updateBECharts() {
        getBE_CPU_IDLE();
        getBE_Mem();
        getBE_DiskIO();
        getBE_base_compaction_score();
        getBE_cumu_compaction_score();
    }
    function getBENodes() {
        MonitorAPI.getBENodes().then(res => {
            console.log(res.data);
            setBENodes(res.data);
            setSelectedBENodes(res.data);
        });
    }
    function getBE_CPU_IDLE() {
        MonitorAPI.getBE_CPU_IDLE(currentTime.start, currentTime.end, {
            nodes: selectedBENodes,
        }).then(res => {
            const option = formatData(res.data, 'CPU空闲率(%)');
            setBE_CPU_IDLE(option);
        });
    }
    function getBE_Mem() {
        MonitorAPI.getBE_Mem(currentTime.start, currentTime.end, {
            nodes: selectedBENodes,
        }).then(res => {
            const option = formatData(res.data, '节点内存使用量');
            setBE_Mem(option);
        });
    }
    function getBE_DiskIO() {
        MonitorAPI.getBE_DiskIO(currentTime.start, currentTime.end, {
            nodes: selectedBENodes,
        }).then(res => {
            const option = formatData(res.data, 'IO利用率');
            setBE_DISK_IO(option);
        });
    }
    function getBE_base_compaction_score() {
        MonitorAPI.getBE_base_compaction_score(currentTime.start, currentTime.end, {
            nodes: selectedBENodes,
        }).then(res => {
            const option = formatData(res.data, '基线数据版本合并情况');
            option.legend.left = 20;
            option.yAxis = {
                type: 'value',
                minInterval: 1,

                boundaryGap: [0, 0.1],
            };
            setBE_base_compaction_score(option);
        });
    }
    function getBE_cumu_compaction_score() {
        MonitorAPI.getBE_cumu_compaction_score(currentTime.start, currentTime.end, {
            nodes: selectedBENodes,
        }).then(res => {
            const option = formatData(res.data, '增量数据版本合并情况');
            option.legend.left = 20;
            option.yAxis = {
                type: 'value',
                minInterval: 1,

                boundaryGap: [0, 0.1],
            };
            setBE_cumu_compaction_score(option);
        });
    }
    return (
        <div style={{ background: '#fafafa' }}>
            <Row justify="space-between" align="middle">
                <Col> </Col>
                <Row justify="start" align="middle" style={{ marginTop: '4px' }}>
                    <Col>{t`viewTime`}: </Col>
                    <Col style={{ margin: '0 10px' }}>
                        <Select
                            style={{ width: 200 }}
                            value={currentTime.value}
                            onChange={(v: string) => {
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
                    <Panel header={t`BENodeStatusMonitoring`} key="1">
                        <Row justify="start" align="middle" style={{ padding: '20px 20px' }}>
                            <span>{t`nodeSelection`}：</span>
                            <Select
                                mode="multiple"
                                allowClear
                                style={{ width: 600 }}
                                placeholder={t`PleaseSelectNode`}
                                defaultValue={[...beNodes]}
                                onChange={handleBENodeChange}
                            >
                                {beNodes.map(item => (
                                    <Option key={item} value={item}>
                                        {item}
                                    </Option>
                                ))}
                            </Select>
                        </Row>
                        <Row justify="start">
                            <Row style={{ height: 340, width: 'calc(100% / 3)' }}>
                                <h4>{t`CPUidleRate`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={be_cpu_idle}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 340, width: '100%', top: '-20px' }}
                                />
                            </Row>
                            <Row style={{ height: 340, width: 'calc(100% / 3)' }}>
                                <h4>{t`nodeMemoryUsage`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={be_mem}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 340, width: '100%', top: '-20px' }}
                                />
                            </Row>
                            <Row style={{ height: 340, width: 'calc(100% / 3)' }}>
                                <h4>{t`IOutilization`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={be_disk_io}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 340, width: '100%', top: '-20px' }}
                                />
                            </Row>
                        </Row>
                        <Divider />
                        <Row justify="start">
                            <Row style={{ height: 340, width: 'calc(100% / 2)' }}>
                                <h4 style={{ marginLeft: '25px' }}>{t`BaselineDataVersionConsolidation`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={be_base_compaction_score}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 340, width: '100%', top: '-20px' }}
                                />
                            </Row>
                            <Row style={{ height: 340, width: 'calc(100% / 2)' }}>
                                <h4 style={{ marginLeft: '25px' }}>{t`IncrementalDataVersionConsolidation`}</h4>
                                <ReactEChartsCore
                                    echarts={echarts}
                                    option={be_cumu_compaction_score}
                                    notMerge={true}
                                    lazyUpdate={true}
                                    style={{ height: 340, width: '100%', top: '-20px' }}
                                />
                            </Row>
                        </Row>
                    </Panel>
                </Collapse>
            </div>
        </div>
    );
}
