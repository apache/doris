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

import Vue from 'vue';
import ElementUI from 'element-ui';
import 'element-ui/lib/theme-chalk/index.css';
import App from './App';
import router from './router';
import store from './vuex/store';
import './assets/icon/iconfont.css'
import axios from 'axios';
Vue.prototype.$axios = axios;


Vue.config.productionTip = false;

Vue.use(ElementUI);
import * as custom from './utils/util'

Object.keys(custom).forEach(key => {
    Vue.filter(key, custom[key])
})

// axios.defaults.headers.common['Authorization'] = "root:12345678";
Vue.prototype.baseurl='/';
new Vue({
    el: '#app',
    router,
    store,
    components: { App },
    template: '<App/>',
    data: {
        Bus: new Vue()
    }

})
