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

/**
* left menu
*/
<template>
  <el-menu default-active="2" :collapse="collapsed" collapse-transition router :default-active="$route.path" unique-opened class="el-menu-vertical-demo" background-color="#334157" text-color="#fff" active-text-color="#ffd04b">
    <div class="logobox">
      <img class="logoimg" src="../assets/img/logo.png" alt="">
    </div>
    <el-submenu v-for="menu in allmenu" :key="menu.menuid" :index="menu.menuname">
      <template slot="title">
        <i class="iconfont" :class="menu.icon"></i>
        <span>{{menu.menuname}}</span>
      </template>
      <el-menu-item-group>
        <el-menu-item v-for="chmenu in menu.menus" :index="'/'+chmenu.url" :key="chmenu.menuid">
          <i class="iconfont" :class="chmenu.icon"></i>
          <span>{{chmenu.menuname}}</span>
        </el-menu-item>
      </el-menu-item-group>
    </el-submenu>
  </el-menu>
</template>
<script>
import { menu } from '../api/doris'
export default {
  name: 'leftnav',
  data() {
    return {
      collapsed: false,
      allmenu: []
    }
  },

  created() {
    let res = {
      success: true,
      data: [
        {
          menuid: 1,
          icon: 'iconshouye',
          menuname: 'Home',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 2,
              icon: 'iconshouye',
              menuname: 'Home',
              hasThird: 'N',
              url: 'home/home',
              menus: null
            }
          ]
        },
        {
          menuid: 33,
          icon: 'iconchaxun',
          menuname: 'Query List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 34,
              icon: 'iconchaxun',
              menuname: 'Query List',
              hasThird: 'N',
              url: 'query/Query',
              menus: null
            }
          ]
        },
        {
          menuid: 71,
          icon: 'iconxitong',
          menuname: 'System',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 89,
              icon: 'icontongji',
              menuname: 'Statistic',
              hasThird: 'N',
              url: 'system/statistic',
              menus: null
            },{
              menuid: 72,
              icon: 'iconguanli',
              menuname: 'Frontends',
              hasThird: 'N',
              url: 'system/Frontends',
              menus: null
            },
            {
              menuid: 174,
              icon: 'iconStocksandbrokerages',
              menuname: 'Brokers',
              hasThird: 'N',
              url: 'system/Brokers',
              menus: null
            },
            {
              menuid: 73,
              icon: 'iconrenzheng',
              menuname: 'Auth',
              hasThird: 'N',
              url: 'system/Auth',
              menus: null
            },
            {
              menuid: 74,
              icon: 'iconload',
              menuname: 'Routine Load',
              hasThird: 'N',
              url: 'system/Routineloads',
              menus: null
            },
            {
              menuid: 75,
              icon: 'iconjob',
              menuname: 'Jobs',
              hasThird: 'N',
              url: 'job/Jobs',
              menus: null
            },
            {
              menuid: 76,
              icon: 'iconziyuan30',
              menuname: 'Resources',
              hasThird: 'N',
              url: 'system/Resources',
              menus: null
            },
            {
              menuid: 77,
              icon: 'iconjiankong',
              menuname: 'Monitor',
              hasThird: 'N',
              url: 'system/Monitor',
              menus: null
            },
            {
              menuid: 78,
              icon: 'iconshiwuguanli',
              menuname: 'Transactions',
              hasThird: 'N',
              url: 'trans/Transactions',
              menus: null
            },
            {
              menuid: 79,
              icon: 'icongroup',
              menuname: 'Colocation Group',
              hasThird: 'N',
              url: 'system/colocation_group',
              menus: null
            },
            {
              menuid: 80,
              icon: 'iconjiedian',
              menuname: 'Backends',
              hasThird: 'N',
              url: 'system/Backends',
              menus: null
            },
            {
              menuid: 81,
              icon: 'iconfuzaijunheng',
              menuname: 'Cluster Balance',
              hasThird: 'N',
              url: 'system/cluster_balance',
              menus: null
            },
            {
              menuid: 82,
              icon: 'iconchaxun',
              menuname: 'Current Queries',
              hasThird: 'N',
              url: 'system/current_queries',
              menus: null
            },
            {
              menuid: 83,
              icon: 'icondb',
              menuname: 'dbs',
              hasThird: 'N',
              url: 'dbs/dbs',
              menus: null
            },
            {
              menuid: 84,
              icon: 'iconshili',
              menuname: 'Current BE Instance',
              hasThird: 'N',
              url: 'system/current_backend_instances',
              menus: null
            },
            {
              menuid: 85,
              icon: 'icontask',
              menuname: 'Tasks',
              hasThird: 'N',
              url: 'system/tasks',
              menus: null
            }
          ]


        },
        {
          menuid: 103,
          icon: 'iconagent_session',
          menuname: 'Session List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 134,
              icon: 'iconagent_session',
              menuname: 'Session List',
              hasThird: 'N',
              url: 'session/Session',
              menus: null
            }
          ]
        },
        {
          menuid: 333,
          icon: 'iconjiedian',
          menuname: 'Backend List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 334,
              icon: 'iconjiedian',
              menuname: 'Backend List',
              hasThird: 'N',
              url: 'system/Backends',
              menus: null
            }
          ]
        },
        {
          menuid: 533,
          icon: 'iconpingtaibianliang',
          menuname: 'Variable List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 534,
              icon: 'iconpingtaibianliang',
              menuname: 'Variable List',
              hasThird: 'N',
              url: 'variable/Variable',
              menus: null
            }
          ]
        },
        {
          menuid: 433,
          icon: 'iconahasyingyonggaokeyong',
          menuname: 'Frontend HA',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 434,
              icon: 'iconahasyingyonggaokeyong',
              menuname: 'Frontend HA',
              hasThird: 'N',
              url: 'ha/ha',
              menus: null
            }
          ]
        },{
          menuid: 233,
          icon: 'iconrizhi',
          menuname: 'Sys Log',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 234,
              icon: 'iconrizhi',
              menuname: 'Sys Log',
              hasThird: 'N',
              url: 'syslog/SystemLog',
              menus: null
            }
          ]
        },
        {
          menuid: 128,
          icon: 'iconbangzhu',
          menuname: 'Help',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 129,
              icon: 'iconbangzhu',
              menuname: 'Help',
              hasThird: 'N',
              url: 'help/help',
              menus: null
            }
          ]
        }
      ],
      msg: 'success'
    }
    this.allmenu = res.data
    this.$root.Bus.$on('toggle', value => {
      this.collapsed = !value
    })
  }
}
</script>
<style>
.el-menu-vertical-demo:not(.el-menu--collapse) {
  width: 240px;
  min-height: 400px;
}
.el-menu-vertical-demo:not(.el-menu--collapse) {
  border: none;
  text-align: left;
}
.el-menu-item-group__title {
  padding: 0px;
}
.el-menu-bg {
  background-color: #1f2d3d !important;
}
.el-menu {
  border: none;
}
.logobox {
  height: 40px;
  line-height: 40px;
  color: #9d9d9d;
  font-size: 20px;
  text-align: center;
  padding: 20px 0px;
}
.logoimg {
  height: 40px;
}
</style>
