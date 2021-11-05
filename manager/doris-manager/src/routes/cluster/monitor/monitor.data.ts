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

import dayjs from 'dayjs';
export function getTimes(now: dayjs.Dayjs) {
    return [
        {
            value: '1',
            text: '最近半小时',
            end: new Date().getTime(),
            start: now.subtract(30, 'minute').valueOf(),
            format: 'HH:mm',
        },
        {
            value: '2',
            text: '最近1小时',
            end: new Date().getTime(),
            start: now.subtract(1, 'hour').valueOf(),
            format: 'HH:mm',
        },
        {
            value: '3',
            text: '最近6小时',
            end: new Date().getTime(),
            start: now.subtract(6, 'hour').valueOf(),
            format: 'HH:mm',
        },
        {
            value: '4',
            text: '最近1天',
            end: new Date().getTime(),
            start: now.subtract(1, 'day').valueOf(),
            format: 'HH:mm',
        },
        {
            value: '5',
            text: '最近三天',
            end: new Date().getTime(),
            start: now.subtract(3, 'day').valueOf(),
            format: 'MM/DD HH:mm',
        },
        {
            value: '6',
            text: '最近一周',
            end: new Date().getTime(),
            start: now.subtract(1, 'week').valueOf(),
            format: 'MM/DD HH:mm',
        },
        {
            value: '7',
            text: '最近半个月',
            end: new Date().getTime(),
            start: now.subtract(15, 'day').valueOf(),
            format: 'MM/DD HH:mm',
        },
        {
            value: '8',
            text: '最近一个月',
            end: new Date().getTime(),
            start: now.subtract(1, 'month').valueOf(),
            format: 'MM/DD HH:mm',
        },
    ];
}

export const CHARTS_OPTIONS = {
    title: { text: '' },
    grid: {
        bottom: 100,
    },
    legend: {
        data: [],
        left: 0,
        bottom: '0',
        height: 80,
        width: '100%',
        type: 'scroll',
        itemHeight: 10,
        orient: 'vertical',
        pageIconSize: 7,
        textStyle: {
            overflow: 'breakAll',
        },
    },
    tooltip: {
        trigger: 'axis',
    },
    xAxis: {
        type: 'category',
        boundaryGap: false,
        data: [],
    },
    yAxis: {
        type: 'value',
    },
    series: [],
};
