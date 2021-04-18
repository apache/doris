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
 
import React, {HTMLAttributes} from 'react';
import classNames from 'classnames';
import {Link} from 'react-router-dom';
import './style.less';

interface FlatBtnProps extends HTMLAttributes<HTMLAnchorElement> {
    to?: string;
    type?: '' | 'danger' | 'warn';
    disabled?: boolean;
    children?: string | JSX.Element;
    className?: string;
    default?: string;
    key?: string | number;
    href?: string;
    [attr: string]: any;
}

const FlatBtn = (props: FlatBtnProps) => {
    if (props.to) {
        return (
            <Link
                {...props}
                to={props.to}
                className={classNames(
                    props.className && props.className,
                    {[`btn-${props.type}`]: props.type},
                    {'flat-btn-disabled': props.disabled},
                    {'flat-btn-default': props.default},
                )}>
                {props.children}
            </Link>
        );
    }
    return (
        <a
            {...props}
            className={classNames(
                props.className && props.className,
                {[`btn-${props.type}`]: props.type},
                {'flat-btn-disabled': props.disabled},
                {'flat-btn-default': props.default},
            )}>
            {props.children}
        </a>
    );
};

export default FlatBtn;
 
