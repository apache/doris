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

class EventEmitter {
    public listeners: any;
    constructor() {
        this.listeners = {};
    }

    on(type: string, cb: any) {
        let cbs = this.listeners[type];
        if (!cbs) {
            cbs = [];
        }
        cbs.push(cb);
        this.listeners[type] = cbs;
        return () => {
            this.remove(type, cb);
        };
    }

    emit(type: any, ...args: any) {
        const cbs = this.listeners[type];
        if (Array.isArray(cbs)) {
            for (let i = 0; i < cbs.length; i++) {
                const cb = cbs[i];
                if (typeof cb === 'function') {
                    cb(...args);
                }
            }
        }
    }

    remove(type: any, cb: any) {
        if (cb) {
            let cbs = this.listeners[type];
            cbs = cbs.filter((eMap: any) => eMap.cb !== cb);
            this.listeners[type] = cbs;
        } else {
            this.listeners[type] = null;
            delete this.listeners[type];
        }
    }
}

export default new EventEmitter();
