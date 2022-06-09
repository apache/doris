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
  <main class="page" :style="pageStyle">
    <ModuleTransition delay="0.08">
      <section v-show="recoShowModule">
        <div class="page-title">
          <h1 class="title">{{$page.title}}</h1>
          <PageInfo :pageInfo="$page" :showAccessNumber="showAccessNumber"></PageInfo>
        </div>
        <!-- 这里使用 v-show，否则影响 SSR -->
        <Content class="theme-reco-content" />
      </section>
    </ModuleTransition>

    <ModuleTransition delay="0.16">
      <footer v-if="recoShowModule" class="page-edit">
        <div class="edit-link" v-if="editLink">
          <a
            :href="editLink"
            target="_blank"
            rel="noopener noreferrer"
          >{{ editLinkText }}</a>
          <OutboundLink/>
        </div>

        <div
          class="last-updated"
          v-if="lastUpdated"
        >
          <span class="prefix">{{ lastUpdatedText }}: </span>
          <span class="time">{{ lastUpdated }}</span>
        </div>
      </footer>
    </ModuleTransition>

    <ModuleTransition delay="0.24">
      <div class="page-nav" v-if="recoShowModule && (prev || next)">
        <p class="inner">
          <span v-if="prev" class="prev">
            <router-link v-if="prev" class="prev" :to="prev.path">
              {{ prev.title || prev.path }}
            </router-link>
          </span>
          <span v-if="next" class="next">
            <router-link v-if="next" :to="next.path">
              {{ next.title || next.path }}
            </router-link>
          </span>
        </p>
      </div>
    </ModuleTransition>

    <ModuleTransition delay="0.32">
      <Comments v-if="recoShowModule" :isShowComments="shouldShowComments"/>
    </ModuleTransition>

    <ModuleTransition>
      <SubSidebar v-if="recoShowModule" class="side-bar" />
    </ModuleTransition>

    <ModuleTransition>
      <PageFooter v-if="$frontmatter.page !== 'home'" />
    </ModuleTransition>
  </main>
</template>

<script>
import { defineComponent, computed, toRefs } from 'vue-demi'
import PageInfo from '@theme/components/PageInfo'
import { resolvePage, outboundRE, endingSlashRE } from '@theme/helpers/utils'
import { ModuleTransition } from '@vuepress-reco/core/lib/components'
import SubSidebar from '@theme/components/SubSidebar'
import { useInstance } from '@theme/helpers/composable'
import PageFooter from '@theme/components/PageFooter'

export default defineComponent({
  components: { PageInfo, ModuleTransition, SubSidebar, PageFooter },

  props: ['sidebarItems'],

  setup (props, ctx) {
    const instance = useInstance()

    const { sidebarItems } = toRefs(props)

    const recoShowModule = computed(() => instance.$parent.recoShowModule)

    // 是否显示评论
    const shouldShowComments = computed(() => {
      const { isShowComments } = instance.$frontmatter
      const { showComment } = instance.$themeConfig.valineConfig || { showComment: true }
      return (showComment !== false && isShowComments !== false) || (showComment === false && isShowComments === true)
    })

    const showAccessNumber = computed(() => {
      const {
        $themeConfig: { valineConfig },
        $themeLocaleConfig: { valineConfig: valineLocalConfig }
      } = instance || {}

      const vc = valineLocalConfig || valineConfig

      return vc && vc.visitor != false
    })

    const lastUpdated = computed(() => {
      if (instance.$themeConfig.lastUpdated === false) return false
      return instance.$page.lastUpdated
    })

    const lastUpdatedText = computed(() => {
      if (typeof instance.$themeLocaleConfig.lastUpdated === 'string') {
        return instance.$themeLocaleConfig.lastUpdated
      }
      if (typeof instance.$themeConfig.lastUpdated === 'string') {
        return instance.$themeConfig.lastUpdated
      }
      return 'Last Updated'
    })

    const prev = computed(() => {
      const frontmatterPrev = instance.$frontmatter.prev
      if (frontmatterPrev === false) {
        return
      } else if (frontmatterPrev) {
        return resolvePage(instance.$site.pages, frontmatterPrev, instance.$route.path)
      } else {
        return resolvePrev(instance.$page, sidebarItems.value)
      }
    })

    const next = computed(() => {
      const frontmatterNext = instance.$frontmatter.next
      if (next === false) {
        return
      } else if (frontmatterNext) {
        return resolvePage(instance.$site.pages, frontmatterNext, instance.$route.path)
      } else {
        return resolveNext(instance.$page, sidebarItems.value)
      }
    })

    const editLink = computed(() => {
      if (instance.$frontmatter.editLink === false) {
        return false
      }
      const {
        repo,
        editLinks,
        docsDir = '',
        docsBranch = 'master',
        docsRepo = repo
      } = instance.$themeConfig

      if (docsRepo && editLinks && instance.$page.relativePath) {
        return createEditLink(repo, docsRepo, docsDir, docsBranch, instance.$page.relativePath)
      }
      return ''
    })

    const editLinkText = computed(() => {
      return (
        instance.$themeLocaleConfig.editLinkText || instance.$themeConfig.editLinkText || `Edit this page`
      )
    })

    const pageStyle = computed(() => {
      return instance.$showSubSideBar ? {} : { paddingRight: '0' }
    })

    return {
      recoShowModule,
      shouldShowComments,
      showAccessNumber,
      lastUpdated,
      lastUpdatedText,
      prev,
      next,
      editLink,
      editLinkText,
      pageStyle
    }
  }
})

function createEditLink (repo, docsRepo, docsDir, docsBranch, path) {
  const bitbucket = /bitbucket.org/
  if (bitbucket.test(repo)) {
    const base = outboundRE.test(docsRepo)
      ? docsRepo
      : repo
    return (
      base.replace(endingSlashRE, '') +
        `/src` +
        `/${docsBranch}/` +
        (docsDir ? docsDir.replace(endingSlashRE, '') + '/' : '') +
        path +
        `?mode=edit&spa=0&at=${docsBranch}&fileviewer=file-view-default`
    )
  }

  const base = outboundRE.test(docsRepo)
    ? docsRepo
    : `https://github.com/${docsRepo}`

  return (
    base.replace(endingSlashRE, '') +
    `/edit` +
    `/${docsBranch}/` +
    (docsDir ? docsDir.replace(endingSlashRE, '') + '/' : '') +
    path
  )
}

function resolvePrev (page, items) {
  return find(page, items, -1)
}

function resolveNext (page, items) {
  return find(page, items, 1)
}

function find (page, items, offset) {
  const res = []
  flatten(items, res)
  for (let i = 0; i < res.length; i++) {
    const cur = res[i]
    if (cur.type === 'page' && cur.path === decodeURIComponent(page.path)) {
      return res[i + offset]
    }
  }
}

function flatten (items, res) {
  for (let i = 0, l = items.length; i < l; i++) {
    if (items[i].type === 'group') {
      flatten(items[i].children || [], res)
    } else {
      res.push(items[i])
    }
  }
}

</script>

<style lang="stylus">
@require '../styles/wrapper.styl'

.page
  position relative
  padding-top 5rem
  padding-bottom 2rem
  padding-right 14rem
  display block
  .side-bar
    position fixed
    top 10rem
    bottom 10rem
    right 2rem
    overflow-y scroll
    &::-webkit-scrollbar
      width: 0
      height: 0
  .page-title
    max-width: $contentWidth;
    margin: 0 auto;
    padding: 1rem 2.5rem;
    color var(--text-color)
  .theme-reco-content h2
    position relative
    padding-left 0.8rem
    &::before
      position absolute
      left 0
      top 3.5rem
      display block
      height 1.8rem
      content ''
      border-left 5px solid $accentColor
  .page-edit
    @extend $wrapper
    padding-top 1rem
    padding-bottom 1rem
    overflow auto
    .edit-link
      display inline-block
      a
        color $accentColor
        margin-right 0.25rem
    .last-updated
      float right
      font-size 0.9em
      .prefix
        font-weight 500
        color $accentColor
      .time
        font-weight 400
        color #aaa
  .comments-wrapper
    @extend $wrapper

.page-nav
  @extend $wrapper
  padding-top 1rem
  padding-bottom 0
  .inner
    min-height 2rem
    margin-top 0
    border-top 1px solid var(--border-color)
    padding-top 1rem
    overflow auto // clear float
  .next
    float right

@media (max-width: $MQMobile)
  .page
    padding-right 0
    .side-bar
      display none
    .page-title
      padding: 0 1rem;
    .page-edit
      .edit-link
        margin-bottom .5rem
      .last-updated
        font-size .8em
        float none
        text-align left

</style>
