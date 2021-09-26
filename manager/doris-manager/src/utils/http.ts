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

/** @format */

import axios, { AxiosRequestConfig, AxiosResponse, AxiosError } from 'axios';
import { message } from 'antd';
import { IResult } from '../interfaces/http.interface';
import { ANALYTICS_URL } from '@src/common/common.data';

interface HTTPConfig extends AxiosRequestConfig {
    message?: string;
}

// const { SERVER_URL } = process.env;

axios.defaults.baseURL = ''; //SERVER_URL;
axios.defaults.headers.post['Content-Type'] = 'application/json;charset=UTF-8';

axios.defaults.timeout = 30000;

axios.interceptors.request.use(
    (config: AxiosRequestConfig): AxiosRequestConfig => {
        const newConfig = Object.assign({ withCredentials: true }, config);
        return newConfig;
    },
    (error: AxiosError) => {
        message.error('网络请求出错');
        return Promise.reject(error);
    },
);

axios.interceptors.response.use(
    (result): AxiosResponse<IResult<any>> => {
        if (result.data.code !== 0) {
            // console.log(result);
            if(result.data.code === 401){
                window.localStorage.removeItem('login');
                window.localStorage.removeItem('user');
            }
        }
        return result;
    },
    (error): Promise<never> => {
        console.error(error.response);
        if (error.response) {
            const text = `网络请求出错 ${error.response.status}`;
            message.error(text);
        }
        return Promise.reject(error.response);
    },
);

class HttpClient {
    async get<T = any>(url: string, data?: any, config: HTTPConfig = {}): Promise<IResult<T>> {
        if (data) {
            config.params = data;
        }
        const result: AxiosResponse = await axios.get(url, config);
        if (result.data.code === 401) {
            window.location.href = ANALYTICS_URL;
        }
        return result.data;
    }

    async post<T = any>(url: string, data?: any, config: HTTPConfig = {}): Promise<IResult<T>> {
        const result: AxiosResponse = await axios.post(url, data, config);
        return result.data;
    }

    async upload(data: any, url: string, config: HTTPConfig = {}): Promise<any> {
        return axios
            .post(url, data, { headers: { 'Content-Type': 'multipart/form-data' }, ...config })
            .then(res => res.data)
            .catch(err => {
                console.log(err);
                message.warn(config.message);
            });
    }
    async put<T = any>(url: string, data?: any, config: HTTPConfig = {}): Promise<IResult<T>> {
        const result: AxiosResponse = await axios.put(url, data);
        return result.data;
    }
    async delete<T = any>(url: string, data?: any, config: HTTPConfig = {}): Promise<IResult<T>> {
        const result: AxiosResponse = await axios.delete(url, data);
        return result.data;
    }
}

export const http = new HttpClient();

export const isSuccess = <T>(res: IResult<T>) => res.code === 0;
