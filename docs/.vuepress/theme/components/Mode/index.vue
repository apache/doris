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
	<div v-click-outside="hideMenu" class="color-picker" v-if="$themeConfig.modePicker !== false" >
		<a class="color-button" @click.prevent="showMenu = !showMenu">
      <reco-icon icon="reco-color" />
		</a>
		<ModuleTransition :transform=" ['translate(-50%, 0)', 'translate(-50%, -10px)']">
			<div v-show="showMenu" class="color-picker-menu">
				<ModePicker />
			</div>
		</ModuleTransition>
	</div>
</template>

<script>
import { RecoIcon, ModuleTransition } from '@vuepress-reco/core/lib/components'
import ClickOutside from 'vue-click-outside'
import ModePicker from './ModePicker'
import applyMode from './applyMode'

export default {
  name: 'UserSettings',

  directives: {
    'click-outside': ClickOutside
  },

  components: {
    ModePicker,
    RecoIcon,
    ModuleTransition
  },

  data () {
    return {
      showMenu: false
    }
  },

  // 为了在保证 modePicker 在 SSR 中正确开关，并实现管理，Mode 组件将负责 modePicker 关闭的情况
  mounted () {
    // modePicker 关闭时默认使用主题设置的模式
    const themeMode = this.$themeConfig.mode || 'auto'
    const { modePicker } = this.$themeConfig
    if (modePicker === false) {
      // 为 'auto' 模式设置监听器
      if (themeMode === 'auto') {
        window.matchMedia('(prefers-color-scheme: dark)').addListener(() => {
          applyMode(themeMode)
        })
        window.matchMedia('(prefers-color-scheme: light)').addListener(() => {
          applyMode(themeMode)
        })
      }
      applyMode(themeMode)
    }
  },

  methods: {
    hideMenu () {
      this.showMenu = false
    }
  }
}
</script>

<style lang="stylus">
.color-picker {
	position: relative;
	margin-right: 1em;
  cursor pointer;
	.color-button {
		align-items: center;
		height: 100%;
		.iconfont {
			font-size 1.4rem
			color: $accentColor
		}
	}

	.color-picker-menu {
		position: absolute;
		top: 40px;
		left: 50%;
		z-index: 150;

		ul {
			list-style-type: none;
			margin: 0;
			padding: 0;
		}
	}
}

@media (max-width: $MQMobile) {
	.color-picker {
		margin-right: 1rem;
		.color-picker-menu {
			left: calc(50% - 35px);
			&::before {
				left: calc(50% + 35px);
			}
		}
	}
}
</style>
