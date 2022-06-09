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
  <div class="mode-options">
    <h4 class="title">Choose mode</h4>
    <ul class="color-mode-options">
      <li
        v-for="(mode, index) in modeOptions"
        :key="index"
        :class="getClass(mode.mode)"
        @click="selectMode(mode.mode)"
      >{{ mode.title }}</li>
    </ul>
  </div>
</template>

<script>
import applyMode from './applyMode'
export default {
  name: 'ModeOptions',

  data () {
    return {
      modeOptions: [
        { mode: 'dark', title: 'dark' },
        { mode: 'auto', title: 'auto' },
        { mode: 'light', title: 'light' }
      ],
      currentMode: 'auto'
    }
  },

  mounted () {
    // modePicker 开启时默认使用用户主动设置的模式
    this.currentMode = localStorage.getItem('mode') || this.$themeConfig.mode || 'auto'

    // Dark and Light autoswitches
    // 为了避免在 server-side 被执行，故在 Vue 组件中设置监听器
    var that = this
    window.matchMedia('(prefers-color-scheme: dark)').addListener(() => {
      that.$data.currentMode === 'auto' && applyMode(that.$data.currentMode)
    })
    window.matchMedia('(prefers-color-scheme: light)').addListener(() => {
      that.$data.currentMode === 'auto' && applyMode(that.$data.currentMode)
    })

    applyMode(this.currentMode)
  },

  methods: {
    selectMode (mode) {
      if (mode !== this.currentMode) {
        this.currentMode = mode
        applyMode(mode)
        localStorage.setItem('mode', mode)
      }
    },
    getClass (mode) {
      return mode !== this.currentMode ? mode : `${mode} active`
    }
  }
}
</script>

<style lang="stylus">
.mode-options
  background-color var(--background-color)
  min-width: 125px;
  margin: 0;
  padding: 1em;
  box-shadow var(--box-shadow);
  border-radius: $borderRadius;
  .title
    margin-top 0
    margin-bottom .6rem
    font-weight bold
    color var(--text-color)
  .color-mode-options
    display: flex;
    flex-wrap wrap
    li
      flex: 1;
      text-align: center;
      font-size 12px
      color var(--text-color)
      line-height 18px
      padding 3px 6px
      border-top 1px solid #666
      border-bottom 1px solid #666
      background-color var(--background-color)
      cursor pointer
      &.dark
        border-radius: $borderRadius 0 0 $borderRadius
        border-left 1px solid #666
      &.light
        border-radius: 0 $borderRadius $borderRadius 0
        border-right 1px solid #666
      &.active
        background-color: $accentColor;
        color #fff
      &:not(.active)
        border-right 1px solid #666
</style>
