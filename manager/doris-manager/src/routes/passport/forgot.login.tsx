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
import Link from 'antd/lib/typography/Link';
import React, { useState } from 'react';
import styles from './index.module.less';

function ForgotLogin(props: any) {
    return (
        <div className={styles['not-found']}>
            <div className={styles['input-gird']}>
                <h3>Please contact an administrator to have them reset your password</h3>
                <br />
                <Link style={{ fontSize: '14px' }} onClick={() => props.history.push(`/login`)}>
                    Back to login
                </Link>
            </div>
        </div>
    );
}

export default ForgotLogin;
