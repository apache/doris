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
 
import LanguageDetector from 'i18next-browser-languagedetector';
import i18n from 'i18next';
import enUsTrans from '../public/locales/en-us.json';
import zhCnTrans from '../public/locales/zh-cn.json';
import {
    initReactI18next
} from 'react-i18next';

i18n.use(LanguageDetector) 
    .use(initReactI18next) 
    .init({
        resources: {
            en: {
                translation: enUsTrans
            },
            zh: {
                translation: zhCnTrans
            }
        },
        fallbackLng: 'en',
        debug: false,
        interpolation: {
            escapeValue: false 
        }
    });

export default i18n;
 
