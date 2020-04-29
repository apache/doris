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
    <br />
    <span class="footer-text">© Copyright 2020, Apache Doris(Incubating)</span>
    <br />
    <span
      class="footer-text"
    >
    Apache Doris(incubating) is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.</span>
  </div>
</template>
<script>
export default {};
</script>
<style lang="stylus">
.footer-wrapper 
  max-width $contentWidth
  text-align: center
  margin 0 auto
  padding 2rem 2.5rem
  color: rgb(93, 93, 93)
  font-weight: 240

@media (min-width: $MQMobile)
  .footer-wrapper 
    padding 2rem
    .footer-text
      font-size: 13px
@media (max-width: $MQMobile)
  .footer-wrapper 
    padding 1.5rem
    .footer-text
      font-size: 12px;
</style>