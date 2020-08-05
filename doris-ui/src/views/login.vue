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

<template>
  <div class="login">
    <el-form
      label-position="left"
      :model="loginForm"
      :rules="rules"
      ref="loginForm"
      label-width="0px"
      class="demo-loginForm login-container"
    >
      <h3 class="title">Apache Doris</h3>
      <el-form-item prop="username">
        <el-input
          type="text"
          v-model="loginForm.username"
          auto-complete="off"
          placeholder="username"
        ></el-input>
      </el-form-item>
      <el-form-item prop="password">
        <el-input
          type="password"
          v-model="loginForm.password"
          auto-complete="off"
          placeholder="password"
        ></el-input>
      </el-form-item>
      <el-form-item style="width:100%;">
        <el-button
          type="primary"
          style="width:100%;"
          @click="submitForm('loginForm')"
          :loading="logining"
        >Login</el-button>
      </el-form-item>
    </el-form>
  </div>
</template>
<script type="text/ecmascript-6">
import axios from "axios";
import { login} from '../api/doris'

import { setCookie, getCookie, delCookie } from "../utils/util";
import md5 from "js-md5";
import Cookies from "js-cookie";

export default {
  name: "login",
  data() {
    return {
      logining: false,
      rememberpwd: false,
      loginForm: {
        username: "",
        password: "",
        authorization: "",
      },
      rules: {
        username: [
          { required: true, message: "please enter username", trigger: "blur" },
        ],
      },
    };
  },
  created() {
    
  },
  methods: {
    getuserpwd() {
      if (getCookie("user") != "" && getCookie("pwd") != "") {
        this.loginForm.username = getCookie("user");
        this.loginForm.password = getCookie("pwd");
        this.rememberpwd = true;
      }
    },
    submitForm(formName) {
      let userNameAndPassword = this.loginForm.username + ":" + this.loginForm.password;
      let encodedData = window.btoa(userNameAndPassword);
      login(encodedData).then(res => {
          if (res.code  === 200) {
           Cookies.set("userName",this.loginForm.username)
           this.$router.push({ path: '/home/home' })
          } else {
             this.$message.error('Login Fail！')
          }
        })
        .catch(err => {
            this.$message.error('Login Fail！')
        })

    },
  },
};
</script>

<style scoped>
.login {
  display: flex;
  justify-content: center;
  align-items: center;
  height: 100%;
  background-image: url("../assets/img/login-background.png");
  background-size: cover;
}
.login-container {
  border-radius: 10px;
  margin: 0px auto;
  width: 350px;
  padding: 30px 35px 15px 35px;
  background: #fff;
  border: 1px solid #eaeaea;
  text-align: left;
  box-shadow: 0 0 20px 2px rgba(0, 0, 0, 0.1);
}
.title {
  margin: 0px auto 40px auto;
  text-align: center;
  color: #505458;
}
.remember {
  margin: 0px 0px 35px 0px;
}
.code-box {
  text-align: right;
}
.codeimg {
  height: 40px;
}
</style>
