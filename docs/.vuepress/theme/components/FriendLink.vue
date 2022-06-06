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
  <div class="friend-link-wrapper">
    <div
      class="friend-link-item"
      v-for="(item, index) in dataAddColor"
      :key="index"
      @mouseenter="showDetail($event)"
      @mouseleave="hideDetail($event)"
      target="_blank">
      <span
        class="list-style"
        :style="{ 'backgroundColor': item.color }">
      </span>
      {{item.title}}
      <transition name="fade">
        <div class="popup-window-wrapper">
          <div
            class="popup-window"
            :style="popupWindowStyle"
            ref="popupWindow">
            <div class="logo">
              <img :src="getImgUrl(item)" />
            </div>
            <div class="info">
              <div class="title">
                <h4>{{ item.title }}</h4>
                <a
                  class="btn-go"
                  :style="{ 'backgroundColor': item.color }"
                  :href="item.link"
                  target="_blank">GO</a>
              </div>
              <p v-if="item.desc">{{ item.desc }}</p>
            </div>
          </div>
        </div>
      </transition>
    </div>
  </div>
</template>

<script>
import { defineComponent, reactive, computed, ref, onMounted } from 'vue-demi'
import md5 from 'md5'
import { getOneColor } from '@theme/helpers/other'
import { useInstance } from '@theme/helpers/composable'

const useDetail = () => {
  const instance = useInstance()
  const isPC = ref(true)

  const popupWindowStyle = reactive({
    left: 0,
    top: 0
  })

  const adjustPosition = (dom) => {
    const { offsetWidth } = document.body
    const { x, width } = dom.getBoundingClientRect()
    const distanceToRight = offsetWidth - (x + width)

    if (distanceToRight < 0) {
      const { offsetLeft } = dom
      popupWindowStyle.left = offsetLeft + distanceToRight + 'px'
    }
  }

  const showDetail = (e) => {
    const currentDom = e.target
    const popupWindowWrapper = currentDom.querySelector('.popup-window-wrapper')
    popupWindowWrapper.style.display = 'block'
    const popupWindow = currentDom.querySelector('.popup-window')
    const infoWrapper = document.querySelector('.info-wrapper')
    const { clientWidth } = currentDom
    const { clientWidth: windowWidth, clientHeight: windowHeight } = popupWindow

    if (isPC) {
      popupWindowStyle.left = (clientWidth - windowWidth) / 2 + 'px'
      popupWindowStyle.top = -windowHeight + 'px'

      infoWrapper.style.overflow = 'visible'

      instance.$nextTick(() => {
        adjustPosition(popupWindow)
      })
    } else {
      const getPosition = function (element) {
        const dc = document
        const rec = element.getBoundingClientRect()
        let _x = rec.left
        let _y = rec.top
        _x += dc.documentElement.scrollLeft || dc.body.scrollLeft
        _y += dc.documentElement.scrollTop || dc.body.scrollTop
        return { left: _x, top: _y }
      }

      infoWrapper.style.overflow = 'hidden'
      const left = getPosition(currentDom).left - getPosition(infoWrapper).left

      popupWindowStyle.left = (-left + (infoWrapper.clientWidth - popupWindow.clientWidth) / 2) + 'px'
      popupWindowStyle.top = -windowHeight + 'px'
    }
  }

  const hideDetail = (e) => {
    const currentDom = e.target.querySelector('.popup-window-wrapper')
    currentDom.style.display = 'none'
  }

  onMounted(() => {
    isPC.value = !/Android|webOS|iPhone|iPod|BlackBerry/i.test(navigator.userAgent)
  })

  return { popupWindowStyle, showDetail, hideDetail }
}

export default defineComponent({
  setup (props, ctx) {
    const instance = useInstance()

    const { popupWindowStyle, showDetail, hideDetail } = useDetail()

    const dataAddColor = computed(() => {
      const { friendLink = [] } = instance && instance.$themeConfig
      return friendLink.map(item => {
        item.color = getOneColor()
        return item
      })
    })

    const getImgUrl = (info) => {
      const { logo = '', email = '' } = info
      if (logo && /^http/.test(logo)) return logo
      if (logo && !/^http/.test(logo)) return instance.$withBase(logo)
      return `//1.gravatar.com/avatar/${md5(email || '')}?s=50&amp;d=mm&amp;r=x`
    }

    return { dataAddColor, popupWindowStyle, showDetail, hideDetail, getImgUrl }
  }
})
</script>

<style lang="stylus" scoped>
.friend-link-wrapper
  position relative
  margin 30px 0
  .friend-link-item
    position relative
    vertical-align: middle;
    margin: 4px 4px 10px;
    padding: 4px 8px 4px 20px;
    line-height 20px
    display: inline-block;
    cursor: default;
    border-radius: $borderRadius
    font-size: 13px;
    box-shadow var(--box-shadow)
    transition: all .5s
    .list-style
      position absolute
      left .4rem
      top 0
      bottom 0
      margin auto
      display block
      width .4rem
      height .4rem
      border-radius .1rem
      background $accentColor
      content ''
    .popup-window-wrapper
      display none
      .popup-window
        position absolute
        display flex
        background var(--background-color)
        box-shadow var(--box-shadow)
        border-radius $borderRadius
        box-sizing border-box
        padding .8rem 1rem
        width 280px
        .logo
          margin-right .4rem
          width 2rem
          height 2rem
          flex 0 0 2rem
          border-radius $borderRadius
          overflow hidden
          img
            width 2rem
            height 2rem
        .info
          flex 0 0 85%
          width 85%
          .title
            display flex
            align-items center
            justify-content space-between
            height 2rem
            h4
              margin .2rem 0
              flex 0 0 86%
              overflow: hidden;
              white-space: nowrap;
              text-overflow: ellipsis;
            .btn-go
              width 1.4rem
              height 1.2rem
              border-radius $borderRadius
              font-size 12px
              color #ffffff
              text-align center
              line-height 1.2rem
              cursor pointer
              transition all .5s
              &:hover
                transform scale(1.1)

.fade-enter-active, .fade-leave-active
  transition opacity .5s
.fade-enter, .fade-leave-to
  opacity 0
</style>
