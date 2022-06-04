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
  <div class="dropdown-wrapper" :class="{ open }">
    <a class="dropdown-title" @click="toggle">
      <span class="title">
        {{ currentItem.text }}
        <reco-icon :icon="`${item.icon}`" />
      </span>
    </a>

    <DropdownTransition>
      <ul class="nav-dropdown" v-show="open">
        <li
          class="dropdown-item"
          :key="subItem.link || index"
          v-for="(subItem, index) in subItems"
        >
          <h4 v-if="subItem.type === 'links'">{{ subItem.text }}</h4>

          <ul
            class="dropdown-subitem-wrapper"
            v-if="subItem.type === 'links'"
          >
            <li
              class="dropdown-subitem"
              :key="childSubItem.link"
              v-for="childSubItem in subItem.items"
            ><NavLink :item="childSubItem"/></li>
          </ul>
          <a class="nav-link" :class="{'router-link-active': subItem.active}" @click="handleClick(subItem)" v-else>{{subItem.text}}</a>
        </li>
      </ul>
    </DropdownTransition>
  </div>
</template>

<script>
import { computed, defineComponent, reactive, ref, watch } from 'vue-demi'
import { RecoIcon } from '@vuepress-reco/core/lib/components'
import NavLink from '@theme/components/NavLink'
import DropdownTransition from '@theme/components/DropdownTransition'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  components: { NavLink, DropdownTransition, RecoIcon },

  props: {
    item: {
      required: true
    }
  },
  data () {
    return {
      subItems: [],
      currentItem: {}
    }
  },
  computed: {
    
  },
  watch: {
    item: {
      immediate: true,
      deep: true,
      handler (val) {
        this.subItems = val.items.map(item => ({...item, active: false}))
        let currentVersion = 'master'
        if (!this.currentItem.text) {
          const versionKeys = this.$themeLocaleConfig.versions.items.map(v => v.text)
          const matchVersion = versionKeys.find(v => this.$route.path.indexOf(v) > -1)
          currentVersion = matchVersion || versionKeys[0]
        } else {
          currentVersion = this.currentItem.text
        }
        const index = this.subItems.findIndex(item => item.text === currentVersion)
        if (index > -1) {
          this.currentItem = this.subItems[index]
          this.currentItem && (this.subItems[index].active = true)
        }
      }
    },
    currentItem: {
      immediate: true,
      handler (val) {
        this.$emit('update-value', val)
      }
    }
  },
  methods: {
    handleClick (item) {
      this.subItems.forEach(v => { v.active = item.text === v.text })
      this.currentItem = item
      this.$router.push(item.link)
    },
  },

  setup (props, ctx) {
    const instance = useInstance()
    const open = ref(false)
    
    const toggle = () => {
      open.value = !open.value
    }

    return { open, toggle }
  }
})
</script>

<style lang="stylus">
.dropdown-wrapper
  cursor pointer
  height 100%
  .dropdown-title
    display block
    height 100%
    color #333
    display flex
    align-items center
    font-size 16px
    font-weight bold
    &:hover
      border-color transparent
    .arrow
      vertical-align middle
      margin-top -1px
      margin-left 0.4rem
  .doris-xiala
    font-size 12px
  .nav-dropdown
    .dropdown-item
      color inherit
      line-height 1.7rem
      h4
        margin 0.45rem 0 0
        border-top 1px solid var(--border-color)
        padding 0.45rem 1.5rem 0 1.25rem
      .dropdown-subitem-wrapper
        padding 0
        list-style none
        .dropdown-subitem
          font-size 0.9em
      a
        display block
        line-height 1.7rem
        position relative
        border-bottom none
        font-weight 400
        margin-bottom 0
        padding 0 1.5rem 0 1.25rem
        &:hover
          color $accentColor
        &.router-link-active
          color $accentColor
          &::after
            content ""
            width 0
            height 0
            border-left 5px solid $accentColor
            border-top 3px solid transparent
            border-bottom 3px solid transparent
            position absolute
            top calc(50% - 2px)
            left 9px
      &:first-child h4
        margin-top 0
        padding-top 0
        border-top 0

@media (max-width: $MQMobile)
  .dropdown-wrapper
    &.open .dropdown-title
      margin-bottom 0.5rem
    .nav-dropdown
      transition height .1s ease-out
      overflow hidden
      .dropdown-item
        h4
          border-top 0
          margin-top 0
          padding-top 0
        h4, & > a
          font-size 15px
          line-height 2rem
        .dropdown-subitem
          font-size 14px
          padding-left 1rem

@media (min-width: $MQMobile)
  .dropdown-wrapper
    &:hover .nav-dropdown
      // override the inline style.
      display block !important
    .dropdown-title .arrow
      // make the arrow always down at desktop
      border-left 4px solid transparent
      border-right 4px solid transparent
      border-top 6px solid var(--text-color-sub)
      border-bottom 0
    .nav-dropdown
      display none
      // Avoid height shaked by clicking
      height auto !important
      box-sizing border-box;
      max-height calc(100vh - 2.7rem)
      overflow-y auto
      position absolute
      top 100%
      right 0
      background-color var(--background-color)
      padding 0.6rem 0
      box-shadow: var(--box-shadow);
      text-align left
      border-radius $borderRadius
      white-space nowrap
      margin 0
</style>
