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
import { Space, Card, Divider, message } from 'antd';
import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';
import { Form, Input, Button, Radio } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import styles from './index.module.less';
import Password from 'antd/lib/input/Password';
type RequiredMark = boolean | 'optional';
import { SpaceAPI } from './space.api';

const FormLayoutDemo = () => {
    const [spaceList, setSpaceList] = useState<{name: string, description: string, id: string}[]>([]);
    const history = useHistory();
    function refresh() {
        SpaceAPI.spaceList().then(res => {
            const { msg, data, code } = res;
            if (code === 0) {
                if (res.data) {
                    setSpaceList(res.data);
                }
            } else {
                message.error(msg);
            }
        });
    }
    const [form] = Form.useForm();
    const [requiredMark, setRequiredMarkType] = useState<RequiredMark>('optional');

    const onRequiredTypeChange = ({ requiredMarkValue }: { requiredMarkValue: RequiredMark }) => {
        setRequiredMarkType(requiredMarkValue);
    };
    useEffect(() => {
        refresh();
    }, []);
    return (
        <div className={styles.dorisSpaceList}>
        <ul className={styles.dorisSpaceListContainer}>
          {spaceList &&
            spaceList.map((item, index) => {
              return (
                <li
                  className="columns"
                  key={item.name + index}
                  onClick={() => {
                    history.push(`/space/check/${item.id}`);
                  }}
                >
                  <div className="bg-white rounded bordered shadowed overflow-hidden">
                    <h3 className="space-title">
                      <span>{item.name}</span>
                    </h3>
                    <p>{item.description}</p>
                  </div>
                </li>
              );
            })}
          <li className="columns text-centered">
            <div className="bg-white rounded bordered shadowed overflow-hidden">
              <a
                className="newSpaceBTN"
                onClick={() => history.push("/space/new")}
              >
                {/* <span className="plus">+</span> */}
                {/* <Icon name={"add"} color="#ccc" size={60} /> */}
                <span>新建空间</span>
              </a>
            </div>
          </li>
        </ul>
      </div>
    );
};
export default FormLayoutDemo;
