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
  <nav class="nav-links" v-if="userLinks.length || repoLink">
    <!-- user links -->
    <div
      class="nav-item"
      v-for="item in userLinks"
      :key="item.link"
    >
      <DropdownLink v-if="item.type === 'links'" :item="item" />
      <NavLink v-else :item="item" />
    </div>

    <!-- repo link -->
    <a
      v-if="repoLink"
      :href="repoLink"
      class="repo-link"
      target="_blank"
      rel="noopener noreferrer"
    >
      <reco-icon :icon="`reco-${repoLabel.toLowerCase()}`" />
      {{ repoLabel }}
      <OutboundLink/>
    </a>
  </nav>
</template>

<script>
import { defineComponent, computed } from 'vue-demi'
import { RecoIcon } from '@vuepress-reco/core/lib/components'
import DropdownLink from '@theme/components/DropdownLink'
import { resolveNavLinkItem } from '@theme/helpers/utils'
import NavLink from '@theme/components/NavLink'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  components: { NavLink, DropdownLink, RecoIcon },

  setup (props, ctx) {
    const instance = useInstance()

    const userNav = computed(() => {
      return instance.$themeLocaleConfig.nav || instance.$themeConfig.nav || []
    })

    const nav = computed(() => {
      const locales = instance.$site.locales || {}

      if (locales && Object.keys(locales).length > 1) {
        const currentLink = instance.$page.path
        const routes = instance.$router.options.routes
        const themeLocales = instance.$themeConfig.locales || {}
        const languageDropdown = {
          text: instance.$themeLocaleConfig.selectText || 'Languages',
          items: Object.keys(locales).map(path => {
            const locale = locales[path]
            const text = themeLocales[path] && themeLocales[path].label || locale.lang
            let link
            // Stay on the current page
            if (locale.lang === instance.$lang) {
              link = currentLink
            } else {
              // Try to stay on the same page
              link = currentLink.replace(instance.$localeConfig.path, path)
              // fallback to homepage
              if (!routes.some(route => route.path === link)) {
                link = path
              }
            }
            return { text, link }
          })
        }

        return [...userNav.value, languageDropdown]
      }

      // blogConfig 的处理，根绝配置自动添加分类和标签
      const blogConfig = instance.$themeConfig.blogConfig || {}
      const isHasCategory = userNav.value.some(item => {
        if (blogConfig.category) {
          return item.text === (blogConfig.category.text || '分类')
        } else {
          return true
        }
      })
      const isHasTag = userNav.value.some(item => {
        if (blogConfig.tag) {
          return item.text === (blogConfig.tag.text || '标签')
        } else {
          return true
        }
      })

      if (!isHasCategory && Object.hasOwnProperty.call(blogConfig, 'category')) {
        const category = blogConfig.category
        const $categories = instance.$categories
        userNav.value.splice(parseInt(category.location || 2) - 1, 0, {
          items: $categories.list.map(item => {
            item.link = item.path
            item.text = item.name
            return item
          }),
          text: category.text || instance.$recoLocales.category,
          type: 'links',
          icon: 'reco-category'
        })
      }

      if (!isHasTag && Object.hasOwnProperty.call(blogConfig, 'tag')) {
        const tag = blogConfig.tag
        userNav.value.splice(parseInt(tag.location || 3) - 1, 0, {
          link: '/tag/',
          text: tag.text || instance.$recoLocales.tag,
          type: 'links',
          icon: 'reco-tag'
        })
      }

      return userNav.value
    })

    const userLinks = computed(() => {
      return (instance.nav || []).map(link => {
        return Object.assign(resolveNavLinkItem(link), {
          items: (link.items || []).map(resolveNavLinkItem)
        })
      })
    })

    const repoLink = computed(() => {
      const { repo } = instance.$themeConfig

      if (repo) {
        return /^https?:/.test(repo)
          ? repo
          : `https://github.com/${repo}`
      }

      return ''
    })

    const repoLabel = computed(() => {
      if (!instance.repoLink) return ''
      if (instance.$themeConfig.repoLabel) {
        return instance.$themeConfig.repoLabel
      }

      const repoHost = instance.repoLink.match(/^https?:\/\/[^/]+/)[0]
      const platforms = ['GitHub', 'GitLab', 'Bitbucket']
      for (let i = 0; i < platforms.length; i++) {
        const platform = platforms[i]
        if (new RegExp(platform, 'i').test(repoHost)) {
          return platform
        }
      }

      return 'Source'
    })

    return { userNav, nav, userLinks, repoLink, repoLabel }
  }
})
</script>

<style lang="stylus">
.nav-links
  display inline-block
  a
    line-height 1.4rem
    color var(--text-color)
    &:hover, &.router-link-active
      color $accentColor
      .iconfont
        color $accentColor
  .nav-item
    position relative
    display inline-block
    margin-left 1.5rem
    line-height 2rem
    &:first-child
      margin-left 0
  .repo-link
    margin-left 1.5rem

@media (max-width: $MQMobile)
  .nav-links
    .nav-item, .repo-link
      margin-left 0

@media (min-width: $MQMobile)
  .nav-item > a:not(.external)
    &:hover, &.router-link-active
      margin-bottom -2px
</style>
