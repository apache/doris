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
  <div class="search-box">
    <reco-icon icon="reco-search" />
    <input
      @input="query = $event.target.value"
      aria-label="Search"
      :value="query"
      :class="{ 'focused': focused }"
      :placeholder="placeholder"
      autocomplete="off"
      spellcheck="false"
      @focus="focused = true"
      @blur="focused = false"
      @keyup.enter="go(focusIndex)"
      @keyup.up="onUp"
      @keyup.down="onDown"
      ref="input"
    >
    <ul
      class="suggestions"
      v-if="showSuggestions"
      :class="{ 'align-right': alignRight }"
      @mouseleave="unfocus"
    >
      <li
        class="suggestion"
        v-for="(s, i) in suggestions"
        :key="i"
        :class="{ focused: i === focusIndex }"
        @mousedown="go(i)"
        @mouseenter="focus(i)"
      >
        <a :href="s.path" @click.prevent>
          <span class="page-title">{{ s.title || s.path }}</span>
          <span v-if="s.header" class="header">&gt; {{ s.header.title }}</span>
        </a>
      </li>
    </ul>
  </div>
</template>

<script>
import { defineComponent, reactive, toRefs, computed } from 'vue-demi'
import { RecoIcon } from '@vuepress-reco/core/lib/components'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  components: { RecoIcon },
  setup (props, ctx) {
    const instance = useInstance()

    const state = reactive({
      query: '',
      focused: false,
      focusIndex: 0,
      placeholder: undefined
    })

    const showSuggestions = computed(() => {
      return (
        state.focused && suggestions.value && suggestions.value.length
      )
    })

    const getPageLocalePath = (page) => {
      for (const localePath in instance.$site.locales || {}) {
        if (localePath !== '/' && page.path.indexOf(localePath) === 0) {
          return localePath
        }
      }
      return '/'
    }

    const suggestions = computed(() => {
      const query = state.query.trim().toLowerCase()
      if (!query) {
        return
      }
      const { pages } = instance.$site
      const max = instance.$site.themeConfig.searchMaxSuggestions
      const localePath = instance.$localePath
      const matches = item => (
        item && item.title && item.title.toLowerCase().indexOf(query) > -1
      )
      const res = []
      for (let i = 0; i < pages.length; i++) {
        if (res.length >= max) break
        const p = pages[i]
        // filter out results that do not match current locale
        if (getPageLocalePath(p) !== localePath) {
          continue
        }
        if (matches(p)) {
          res.push(p)
        } else if (p.headers) {
          for (let j = 0; j < p.headers.length; j++) {
            if (res.length >= max) break
            const h = p.headers[j]
            if (matches(h)) {
              res.push(Object.assign({}, p, {
                path: p.path + '#' + h.slug,
                header: h
              }))
            }
          }
        }
      }
      return res
    })

    const alignRight = computed(() => {
      const navCount = (instance.$site.themeConfig.nav || []).length
      const repo = instance.$site.repo ? 1 : 0
      return navCount + repo <= 2
    })

    const onUp = () => {
      if (showSuggestions.value) {
        if (state.focusIndex > 0) {
          state.focusIndex--
        } else {
          state.focusIndex = suggestions.value.length - 1
        }
      }
    }

    const onDown = () => {
      if (showSuggestions.value) {
        if (state.focusIndex < suggestions.value.length - 1) {
          state.focusIndex++
        } else {
          state.focusIndex = 0
        }
      }
    }

    const go = (i) => {
      if (!showSuggestions.value) {
        return
      }
      instance.$router.push(suggestions.value[i].path)
      state.query = ''
      state.focusIndex = 0
    }

    const focus = (i) => {
      state.focusIndex = i
    }

    const unfocus = () => {
      state.focusIndex = -1
    }

    return { showSuggestions, suggestions, alignRight, onUp, onDown, focus, unfocus, go, ...toRefs(state) }
  },
  mounted () {
    this.placeholder = this.$site.themeConfig.searchPlaceholder || ''
  }
})
</script>

<style lang="stylus">
.search-box
  display inline-block
  position relative
  margin-right 1rem
  .iconfont
    position absolute
    top 0
    bottom 0
    z-index 0
    left .6rem
    margin auto
  input
    cursor text
    width 10rem
    height: 2rem
    color lighten($textColor, 25%)
    display inline-block
    border 1px solid var(--border-color)
    border-radius $borderRadius
    font-size 0.9rem
    line-height 2rem
    padding 0 0.5rem 0 2rem
    outline none
    transition all .2s ease
    background transparent
    background-size 1rem
    &:focus
      cursor auto
      border-color $accentColor
  .suggestions
    background var(--background-color)
    width 20rem
    position absolute
    top 1.5rem
    border 1px solid darken($borderColor, 10%)
    border-radius 6px
    padding 0.4rem
    list-style-type none
    &.align-right
      right 0
  .suggestion
    line-height 1.4
    padding 0.4rem 0.6rem
    border-radius 4px
    cursor pointer
    a
      white-space normal
      color var(--text-color)
      .page-title
        font-weight 600
      .header
        font-size 0.9em
        margin-left 0.25em
    &.focused
      background-color var(--border-color)
      a
        color $accentColor
@media (max-width: $MQNarrow)
  .search-box
    input
      cursor pointer
      width 0
      border-color transparent
      position relative
      &:focus
        cursor text
        left 0
        width 10rem
// Match IE11
@media all and (-ms-high-contrast: none)
  .search-box input
    height 2rem
@media (max-width: $MQNarrow) and (min-width: $MQMobile)
  .search-box
    margin-right 0
    .suggestions
      left 0
@media (max-width: $MQMobile)
  .search-box
    margin-right 0
    .suggestions
      right 0
@media (max-width: $MQMobileNarrow)
  .search-box
    .suggestions
      width calc(100vw - 4rem)
    input:focus
      width 8rem
</style>
