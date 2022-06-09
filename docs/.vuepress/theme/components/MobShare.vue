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
  <div>
    <div class="-mob-share-ui-button -mob-share-open">分享</div>
      <div class="-mob-share-ui" style="display: none">
        <ul class="-mob-share-list">
            <li class="-mob-share-weibo"><p>新浪微博</p></li>
            <li class="-mob-share-qzone"><p>QQ空间</p></li>
            <li class="-mob-share-qq"><p>QQ好友</p></li>
            <li class="-mob-share-douban"><p>豆瓣</p></li>
            <li class="-mob-share-facebook"><p>Facebook</p></li>
            <li class="-mob-share-twitter"><p>Twitter</p></li>
        </ul>
        <div class="-mob-share-close">取消</div>
      </div>
      <div class="-mob-share-ui-bg"></div>
  </div>
</template>

<script>
import { defineComponent } from 'vue-demi'
export default defineComponent({
  setup (props, ctx) {
    const script = document.createElement('script')
    script.src = 'http://f1.webshare.mob.com/code/mob-share.js?appkey=2d1a37832f835'
    script.id = '-mob-share'

    document.body.append(script)
  }
})
</script>

