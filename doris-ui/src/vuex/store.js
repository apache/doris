import Vue from 'vue';
import Vuex from 'vuex';

Vue.use(Vuex);
export default new Vuex.Store({
    state: {
        user: false
    },
    mutations: {
        login(state, user) {
            state.user = user;
            localStorage.setItem("userInfo", user);
        },
        logout(state, user) {
            state.user = "";
            localStorage.setItem("userInfo", "");
        }
    }
})