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
          icon: 'li-icon-xiangmuguanli',
          menuname: 'Home',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 2,
              icon: 'icon-cat-skuQuery',
              menuname: 'Home',
              hasThird: 'N',
              url: 'home/home',
              menus: null
            }
          ]
        },
        {
          menuid: 33,
          icon: 'li-icon-dingdanguanli',
          menuname: 'Query List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 34,
              icon: 'icon-order-manage',
              menuname: 'Query List',
              hasThird: 'N',
              url: 'query/Query',
              menus: null
            }
          ]
        },
        {
          menuid: 71,
          icon: 'li-icon-xitongguanli',
          menuname: 'System',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 89,
              icon: 'icon-cus-manage',
              menuname: 'Statistic',
              hasThird: 'N',
              url: 'system/statistic',
              menus: null
            },{
              menuid: 72,
              icon: 'icon-cus-manage',
              menuname: 'Frontends',
              hasThird: 'N',
              url: 'system/Frontends',
              menus: null
            },
            {
              menuid: 174,
              icon: 'icon-cms-manage',
              menuname: 'Brokers',
              hasThird: 'N',
              url: 'system/Brokers',
              menus: null
            },
            {
              menuid: 73,
              icon: 'icon-news-manage',
              menuname: 'Auth',
              hasThird: 'N',
              url: 'system/Auth',
              menus: null
            },
            {
              menuid: 74,
              icon: 'icon-cs-manage',
              menuname: 'Routine Load',
              hasThird: 'N',
              url: 'system/Routineloads',
              menus: null
            },
            {
              menuid: 75,
              icon: 'icon-promotion-manage',
              menuname: 'Jobs',
              hasThird: 'N',
              url: 'job/Jobs',
              menus: null
            },
            {
              menuid: 76,
              icon: 'icon-cms-manage',
              menuname: 'Resources',
              hasThird: 'N',
              url: 'system/Resources',
              menus: null
            },
            {
              menuid: 77,
              icon: 'icon-cms-manage',
              menuname: 'Monitor',
              hasThird: 'N',
              url: 'system/Monitor',
              menus: null
            },
            {
              menuid: 78,
              icon: 'icon-cms-manage',
              menuname: 'Transactions',
              hasThird: 'N',
              url: 'trans/Transactions',
              menus: null
            },
            {
              menuid: 79,
              icon: 'icon-cms-manage',
              menuname: 'Colocation Group',
              hasThird: 'N',
              url: 'system/colocation_group',
              menus: null
            },
            {
              menuid: 80,
              icon: 'icon-cms-manage',
              menuname: 'Backends',
              hasThird: 'N',
              url: 'system/Backends',
              menus: null
            },
            {
              menuid: 81,
              icon: 'icon-cms-manage',
              menuname: 'Cluster Balance',
              hasThird: 'N',
              url: 'system/cluster_balance',
              menus: null
            },
            {
              menuid: 82,
              icon: 'icon-cms-manage',
              menuname: 'Current Queries',
              hasThird: 'N',
              url: 'system/current_queries',
              menus: null
            },
            {
              menuid: 83,
              icon: 'icon-cms-manage',
              menuname: 'dbs',
              hasThird: 'N',
              url: 'dbs/dbs',
              menus: null
            },
            {
              menuid: 84,
              icon: 'icon-cms-manage',
              menuname: 'Current BE Instance',
              hasThird: 'N',
              url: 'system/current_backend_instances',
              menus: null
            },
            {
              menuid: 85,
              icon: 'icon-cms-manage',
              menuname: 'Tasks',
              hasThird: 'N',
              url: 'system/tasks',
              menus: null
            }
          ]


        },
        {
          menuid: 103,
          icon: 'li-icon-dingdanguanli',
          menuname: 'Session List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 134,
              icon: 'icon-order-manage',
              menuname: 'Session List',
              hasThird: 'N',
              url: 'session/Session',
              menus: null
            }
          ]
        },
        {
          menuid: 333,
          icon: 'li-icon-dingdanguanli',
          menuname: 'Backend List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 334,
              icon: 'icon-order-manage',
              menuname: 'Backend List',
              hasThird: 'N',
              url: 'system/Backends',
              menus: null
            }
          ]
        },
        {
          menuid: 533,
          icon: 'li-icon-dingdanguanli',
          menuname: 'Variable List',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 534,
              icon: 'icon-order-manage',
              menuname: 'Variable List',
              hasThird: 'N',
              url: 'variable/Variable',
              menus: null
            }
          ]
        },
        {
          menuid: 433,
          icon: 'li-icon-dingdanguanli',
          menuname: 'Frontend HA',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 434,
              icon: 'icon-order-manage',
              menuname: 'Frontend HA',
              hasThird: 'N',
              url: 'ha/ha',
              menus: null
            }
          ]
        },{
          menuid: 233,
          icon: 'li-icon-dingdanguanli',
          menuname: 'Sys Log',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 234,
              icon: 'icon-order-manage',
              menuname: 'Sys Log',
              hasThird: 'N',
              url: 'log/SystemLog',
              menus: null
            }
          ]
        },     
        {
          menuid: 128,
          icon: 'li-icon-shangchengxitongtubiaozitihuayuanwenjian91',
          menuname: 'Help',
          hasThird: null,
          url: null,
          menus: [
            {
              menuid: 129,
              icon: 'icon-provider-manage',
              menuname: 'Help',
              hasThird: 'N',
              url: 'machine/MachineConfig',
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