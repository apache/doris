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
  <Common class="timeline-wrapper" :sidebar="false">
    <ul class="timeline-content">
      <ModuleTransition >
        <li v-show="recoShowModule" class="desc"><!-- {{$recoLocales.timeLineMsg}} -->Yesterday again!</li>
      </ModuleTransition>
      <ModuleTransition
        :delay="String(0.08 * (index + 1))"
        v-for="(item, index) in $recoPostsForTimeline"
        :key="index">
        <li v-show="recoShowModule">
          <h3 class="year">{{item.year}}</h3>
          <ul class="year-wrapper">
            <li v-for="(subItem, subIndex) in item.data" :key="subIndex">
              <span class="date">{{dateFormat(subItem.frontmatter.date)}}</span>
              <span class="title" @click="go(subItem.path)">{{subItem.title}}</span>
            </li>
          </ul>
        </li>
      </ModuleTransition>
    </ul>
  </Common>
</template>

<script>
import { defineComponent } from 'vue-demi'
import Common from '@theme/components/Common'
import { ModuleTransition } from '@vuepress-reco/core/lib/components'
import moduleTransitonMixin from '@theme/mixins/moduleTransiton'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  name: 'TimeLine',
  mixins: [moduleTransitonMixin],
  components: { Common, ModuleTransition },
  setup (props, ctx) {
    const instance = useInstance()

    const go = (url) => {
      instance.$router.push({ path: url })
    }

    const dateFormat = (date, type) => {
      function renderTime (date) {
        const dateee = new Date(date).toJSON()
        return new Date(+new Date(dateee) + 8 * 3600 * 1000).toISOString().replace(/T/g, ' ').replace(/\.[\d]{3}Z/, '').replace(/-/g, '/')
      }
      date = renderTime(date)
      const dateObj = new Date(date)
      const mon = dateObj.getMonth() + 1
      const day = dateObj.getDate()
      return `${mon}-${day}`
    }

    return { go, dateFormat }
  }
})
</script>

<style src="../styles/theme.styl" lang="stylus"></style>

<style lang="stylus" scoped>
@require '../styles/wrapper.styl'

.timeline-wrapper
  max-width: $contentWidth;
  margin: 0 auto;
  padding: 4.6rem 2.5rem 0;
  .timeline-content
    box-sizing border-box
    position relative
    list-style none
    &::after {
      content: " ";
      position: absolute;
      top: 14px;
      left: 0;
      z-index: -1;
      margin-left: -2px;
      width: 4px;
      height: 100%;
      background: #f5f5f5;
    }
    .desc, .year {
      position: relative;
      color #333;
      font-size 50px
      &:before {
        content: " ";
        position: absolute;
        z-index 2;
        left: -20px;
        top: 50%;
        margin-left: -4px;
        margin-top: -4px;
        width: 8px;
        height: 8px;
        background: #fff;
        border: 1px solid #eee;
        border-radius: 50%;
      }
    }
    .year {
      margin: 80px 0 20px;
      color var(--text-color);
      font-weight: 700;
      font-size 26px
    }
    .year-wrapper {
      padding-left 0!important
      li {
        display flex
        padding 15px 0 15px
        list-style none
        border-bottom: 1px dashed #eee
        position relative
        &:hover {
          .date {
            color $accentColor
            &::before {
              background $accentColor
            }
          }
          .title {
            color $accentColor
          }
        }
        .date {
          width 50px
          line-height 30px
          color #999
          font-size 14px
          &::before {
            content: " ";
            position: absolute;
            left: -18px;
            top: 26px;
            width: 6px;
            height: 6px;
            margin-left: -4px;
            background: #fff;
            border-radius: 50%;
            border: 1px solid #eee;
            z-index 2
          }
        }
        .title {
          line-height 30px
          color #333
          font-size 16px
          cursor pointer
        }
      }
    }
@media (max-width: $MQMobile)
  .timeline-wrapper
    margin: 0 1.2rem;
</style>
