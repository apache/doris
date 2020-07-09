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
