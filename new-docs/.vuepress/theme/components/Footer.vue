<!-- 
Licensed to the Apache Software Foundation (ASF) under one
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
  <div class="footer-wrapper">
    <p class="apache-info">
      <a href="https://apache.org">
        <img width="200" :src="$withBase('/images/asf_logo.svg')" alt="The Apache Software Foundation">
      </a>
      <a style="float: right" href="https://www.apache.org/events/current-event.html">
        <img src="https://www.apache.org/events/current-event-234x60.png">
      </a>
    <p>
      <a href="https://www.apache.org/licenses/">License</a> | <a href="https://www.apache.org/security/">Security</a> | <a href="https://www.apache.org/foundation/thanks.html">Thanks</a> | <a href="https://www.apache.org/foundation/sponsorship.html">Sponsorship</a>
    </p>
    <p>
      <br />
      <span
        class="footer-text"
      >
        Copyright © {{ thisYear }} The Apache Software Foundation. Licensed under the Apache License, Version 2.0. Apache Doris(Incubating), Apache Incubator, Apache, the Apache feather logo, the Apache Doris(Incubating) logo and the Apache Incubator project logo are trademarks of The Apache Software Foundation.
      </span>
      <br />
      <span
        class="footer-text"
      >
        Apache Doris(incubating) is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
      </span>
    </p>
  </div>
</template>
<script>
export default {
  computed: {
    thisYear: () => new Date().getFullYear()
  }
}
</script>
<style lang="stylus">
.footer-wrapper
  font-family -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, "Fira Sans", "Droid Sans", "Helvetica Neue", sans-serif
  max-width $contentWidth
  text-align center
  margin 0 auto
  color rgb(93, 93, 93)
  font-weight: 240
  a
    color #3E8EFF
  .apache-info
    display flex
    flex-direction row
    justify-content space-between
    align-items center
    margin-bottom 1rem
    width 100%
    font-size .75em
    border-collapse collapse
    overflow-x auto
  .footer-text
    display inline-block
    line-height 1.2em

@media (min-width: $MQMobile)
  .footer-wrapper 
    padding 1rem 0rem
    .footer-text
      font-size .9rem
@media (max-width: $MQMobile)
  .footer-wrapper 
    padding 1.5rem
    .footer-text
      font-size 12px
</style>
