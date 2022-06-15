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
  <div class="password-shadow">
    <ModuleTransition>
      <h3 v-show="recoShowModule" class="title">{{isPage ? $frontmatter.title : $site.title || $localeConfig.title}}</h3>
    </ModuleTransition>

    <ModuleTransition delay="0.08">
      <p class="description" v-if="recoShowModule && !isPage">{{$site.description || $localeConfig.description}}</p>
    </ModuleTransition>

    <ModuleTransition delay="0.16">
      <label v-show="recoShowModule" class="inputBox" id="box">
        <input
          v-model="key"
          type="password"
          @keyup.enter="inter"
          @focus="inputFocus"
          @blur="inputBlur">
        <span>{{warningText}}</span>
        <button ref="passwordBtn" @click="inter">OK</button>
      </label>
    </ModuleTransition>

    <ModuleTransition delay="0.24">
      <div v-show="recoShowModule" class="footer">
        <span>
          <reco-icon icon="reco-theme" />
          <a target="blank" href="https://vuepress-theme-reco.recoluan.com">vuePress-theme-reco</a>
        </span>
        <span>
          <reco-icon icon="reco-copyright" />
          <a>
            <span v-if="$themeConfig.author">{{ $themeConfig.author }}</span>
            &nbsp;&nbsp;
            <span v-if="$themeConfig.startYear && $themeConfig.startYear != year">{{ $themeConfig.startYear }} - </span>
            {{ year }}
          </a>
        </span>
      </div>
    </ModuleTransition>
  </div>
</template>

<script>
import { defineComponent, ref, toRefs, computed } from 'vue-demi'
import md5 from 'md5'
import { ModuleTransition, RecoIcon } from '@vuepress-reco/core/lib/components'
import { useInstance } from '@theme/helpers/composable'

export default defineComponent({
  name: 'Password',
  components: { ModuleTransition, RecoIcon },
  props: {
    isPage: {
      type: Boolean,
      default: false
    }
  },
  setup (props, ctx) {
    const instance = useInstance()

    const year = new Date().getFullYear()

    const key = ref('')
    const warningText = ref('Konck! Knock!')
    const recoShowModule = computed(() => instance?.$parent?.recoShowModule)
    const { isPage } = toRefs(props)

    const isHasKey = () => {
      let { keys } = instance.$themeConfig.keyPage
      keys = keys.map(item => item.toLowerCase())
      return keys.indexOf(sessionStorage.getItem('key')) > -1
    }
    const isHasPageKey = () => {
      const pageKeys = instance.$frontmatter.keys.map(item => item.toLowerCase())
      const pageKey = `pageKey${window.location.pathname}`

      return pageKeys && pageKeys.indexOf(sessionStorage.getItem(pageKey)) > -1
    }

    const inter = () => {
      const keyVal = md5(key.value.trim())
      const pageKey = `pageKey${window.location.pathname}`
      const keyName = isPage.value ? pageKey : 'key'
      sessionStorage.setItem(keyName, keyVal)
      const isKeyTrue = isPage.value ? isHasPageKey() : isHasKey()
      if (!isKeyTrue) {
        warningText.value = 'Key Error'
        return
      }

      warningText.value = 'Key Success'

      const width = document.getElementById('box').style.width

      instance.$refs.passwordBtn.style.width = `${width - 2}px`
      instance.$refs.passwordBtn.style.opacity = 1

      setTimeout(() => {
        window.location.reload()
      }, 800)
    }

    const inputFocus = () => {
      warningText.value = 'Input Your Key'
    }

    const inputBlur = () => {
      warningText.value = 'Konck! Knock!'
    }

    return { warningText, year, key, recoShowModule, inter, inputFocus, inputBlur }
  }
})
</script>

<style lang="stylus" scoped>
.password-shadow {
  overflow hidden
  position relative
  background #fff
  background var(--background-color)
  box-sizing border-box
  .title {
    margin 8rem auto 2rem
    width 100%
    text-align center
    font-size 30px
    box-sizing: border-box;
    text-shadow $textShadow
    color $textColor
    color var(--text-color)
  }
  .description {
    margin 0 auto 6rem
    text-align center
    color $textColor
    color var(--text-color)
    font-size 22px
    box-sizing: border-box;
    padding: 0 10px;
    text-shadow $textShadow
  }
  .inputBox{
    position absolute
    top 40%
    left 0
    right 0
    margin auto
    display block
    max-width:700px;
    height: 100px;
    background: $accentColor;
    border-radius: $borderRadius
    padding-left 20px
    box-sizing border-box
    opacity 0.9
    input{
      width:570px;
      height:100%;
      border:none;
      padding:0;
      padding-left:5px;
      color: #fff;
      background: none;
      outline: none;
      position: absolute;
      bottom:0;
      left 20px
      opacity 0
      font-size 50px
      &:focus {
        opacity 1
      }
      &:focus~span{
        transform: translateY(-80px);
        color $accentColor
        font-size 30px
        opacity:0.8;
      }
      &:focus~button{
        opacity:1;
        width:100px;
      }
    }
    span{
      width:200px;
      height: 100%;
      display: block;
      position: absolute;
      line-height:100px;
      top:0;
      left:20px;
      color: #fff;
      cursor: text;
      transition: 0.5s;
      transform-origin: left top;
      font-size 30px
    }
    button{
      overflow hidden
      width:0px;
      height:98px;
      border-radius: $borderRadius
      position: absolute;
      border 1px solid $accentColor
      background var(--background-color)
      right:1px;
      top 1px
      border:0;
      padding:0;
      color: $accentColor;
      font-size:18px;
      outline:none;
      cursor: pointer;
      opacity:0;
      transition: 0.5s;
      z-index: 1;
    }
  }
  .footer {
    position: absolute;
    left 0
    right 0
    bottom 10%
    padding: 2.5rem;
    text-align: center;
    color: lighten($textColor, 25%);
    > span {
      margin-left 1rem
      > i {
        margin-right .5rem
      }
    }
  }
  @media (max-width: $MQMobile) {
    .inputBox{
      max-width:700px;
      height: 60px;
      background: $accentColor;
      border-radius: $borderRadius
      position: absolute;
      left 0
      right 0
      top 43%
      margin auto 20px
      padding-left 0
      box-sizing border-box
      opacity 0.9
      input{
        width: 60%;
        height:100%;
        border:none;
        padding:0;
        padding-left:5px;
        color: #fff;
        background: none;
        outline: none;
        position: absolute;
        bottom:0;
        opacity 0
        font-size 30px
        &:focus {
          opacity 1
        }
        &:focus~span{
          transform: translateY(-60px);
          color $accentColor
          font-size 20px
          opacity:0.8;
        }
        &:focus~button{
          opacity:1;
          width:60px;
        }
      }
      span{
        width:200px;
        height: 100%;
        display: block;
        position: absolute;
        line-height:60px;
        top:0;
        left:20px;
        color: #fff;
        cursor: text;
        transition: 0.5s;
        transform-origin: left top;
        font-size 20px
      }
      button{
        width:0px;
        height:58px;
        border-radius: $borderRadius
        position: absolute;
        border 1px solid $accentColor
        right:1px;
        top 1px
        border:0;
        padding:0;
        background: #fff;
        color: $accentColor;
        font-size:18px;
        outline:none;
        cursor: pointer;
        opacity:0;
        transition: 0.5s;
        z-index: 1;
      }
    }
    .footer {
      margin-left 0

    }
  }
  @media (max-width: $MQNarrow) {
    .footer {
      margin-left 0
    }
  }
}
</style>
