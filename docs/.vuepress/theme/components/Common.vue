<!-- Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
<template>
  <div class="theme-container" :class="pageClasses">
    <div v-if="!absoluteEncryption">
      <transition name="fade">
        <LoadingPage v-show="firstLoad" class="loading-wrapper" />
      </transition>

      <transition name="fade">
        <Password v-show="!firstLoad && !isHasKey" class="password-wrapper-out" key="out" />
      </transition>

      <div :class="{ 'hide': firstLoad || !isHasKey }">
        <Navbar v-if="shouldShowNavbar" @toggle-sidebar="toggleSidebar" />

        <div class="sidebar-mask" @click="toggleSidebar(false)"></div>

        <Sidebar :items="sidebarItems" @toggle-sidebar="toggleSidebar">
          <PersonalInfo slot="top" />
          <slot name="sidebar-bottom" slot="bottom"></slot>
        </Sidebar>

        <Password v-show="!isHasPageKey" :isPage="true" class="password-wrapper-in" key="in"></Password>
        <div :class="{ 'hide': !isHasPageKey }">
          <slot></slot>
        </div>
      </div>
    </div>
    <div v-else>
      <transition name="fade">
        <LoadingPage v-if="firstLoad" />
        <Password v-else-if="!isHasKey" />
        <div v-else>
          <Navbar v-if="shouldShowNavbar" @toggle-sidebar="toggleSidebar"/>

          <div class="sidebar-mask" @click="toggleSidebar(false)"></div>

          <Sidebar :items="sidebarItems" @toggle-sidebar="toggleSidebar">
            <PersonalInfo slot="top" />
            <slot name="sidebar-bottom" slot="bottom"/>
          </Sidebar>

          <Password v-if="!isHasPageKey" :isPage="true"></Password>
          <slot v-else></slot>
        </div>
      </transition>
    </div>
  </div>
</template>

<script>
import { defineComponent, computed, ref, onMounted, toRefs } from 'vue-demi'
import Navbar from '@theme/components/Navbar'
import Sidebar from '@theme/components/Sidebar'
import PersonalInfo from '@theme/components/PersonalInfo'
import Password from '@theme/components/Password'
import { setTimeout } from 'timers'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  components: { Sidebar, Navbar, Password, PersonalInfo },

  props: {
    sidebar: {
      type: Boolean,
      default: true
    },
    sidebarItems: {
      type: Array,
      default: () => []
    },
    showModule: {
      type: Boolean,
      default: false
    }
  },

  setup (props, ctx) {
    const instance = useInstance()

    const isSidebarOpen = ref(false)
    const isHasKey = ref(true)
    const isHasPageKey = ref(true)
    const firstLoad = ref(true)

    const shouldShowSidebar = computed(() => props.sidebarItems.length > 0)
    const absoluteEncryption = computed(() => {
      return instance.$themeConfig.keyPage && instance.$themeConfig.keyPage.absoluteEncryption === true
    })
    const shouldShowNavbar = computed(() => {
      const { themeConfig } = instance.$site
      const { frontmatter } = instance.$page

      if (
        frontmatter.navbar === false ||
        themeConfig.navbar === false
      ) return false

      return (
        instance.$title ||
        themeConfig.logo ||
        themeConfig.repo ||
        themeConfig.nav ||
        instance.$themeLocaleConfig.nav
      )
    })

    const pageClasses = computed(() => {
      const classValue = {
        'no-navbar': !shouldShowNavbar.value,
        'sidebar-open': isSidebarOpen.value,
        'no-sidebar': !shouldShowSidebar.value
      }

      const { pageClass: userPageClass } = instance.$frontmatter || {}
      if (userPageClass) classValue[userPageClass] = true

      return classValue
    })

    const hasKey = () => {
      const { keyPage } = instance.$themeConfig
      if (!keyPage || !keyPage.keys || keyPage.keys.length === 0) {
        isHasKey.value = true
        return
      }

      let { keys } = keyPage
      keys = keys.map(item => item.toLowerCase())
      isHasKey.value = keys && keys.indexOf(sessionStorage.getItem('key')) > -1
    }
    const initRouterHandler = () => {
      instance.$router.afterEach(() => {
        isSidebarOpen.value = false
      })
    }
    const hasPageKey = () => {
      let pageKeys = instance.$frontmatter.keys
      if (!pageKeys || pageKeys.length === 0) {
        isHasPageKey.value = true
        return
      }

      pageKeys = pageKeys.map(item => item.toLowerCase())

      isHasPageKey.value = pageKeys.indexOf(sessionStorage.getItem(`pageKey${window.location.pathname}`)) > -1
    }
    const toggleSidebar = (to) => {
      isSidebarOpen.value = typeof to === 'boolean' ? to : !isSidebarOpen.value
    }
    const handleLoading = () => {
      const time = instance.$frontmatter.home && sessionStorage.getItem('firstLoad') == undefined ? 1000 : 0
      setTimeout(() => {
        firstLoad.value = false
        if (sessionStorage.getItem('firstLoad') == undefined) sessionStorage.setItem('firstLoad', false)
      }, time)
    }

    // 首次渲染时，recoShowModule 直接为 true，否则锚点失效
    const { showModule } = toRefs(props)
    const recoShowModule = computed(() => {
      if (firstLoad.value) {
        return true
      } else {
        return showModule.value
      }
    })

    onMounted(() => {
      initRouterHandler()
      hasKey()
      hasPageKey()
      handleLoading()
    })

    return { isSidebarOpen, absoluteEncryption, shouldShowNavbar, shouldShowSidebar, pageClasses, hasKey, hasPageKey, isHasKey, isHasPageKey, toggleSidebar, firstLoad, recoShowModule }
  },

  watch: {
    $frontmatter (newVal, oldVal) {
      this.hasKey()
      this.hasPageKey()
    }
  }
})
</script>

<style lang="stylus" scoped>
.theme-container
  .loading-wrapper
    position absolute
    z-index 22
    top 0
    bottom 0
    left 0
    right 0
    margin auto
  .password-wrapper-out
    position absolute
    z-index 21
    top 0
    bottom 0
    left 0
    right 0
    margin auto
  .password-wrapper-in
    position absolute
    z-index 8
    top 0
    bottom 0
    left 0
    right 0
  .hide
    height 100vh
    overflow hidden
    opacity 0

.fade-enter-active, .fade-leave-active {
  transition: opacity .5s ease-in-out .5s;
}
.fade-enter, .fade-leave-to /* .fade-leave-active below version 2.1.8 */ {
  opacity: 0;
}
</style>
