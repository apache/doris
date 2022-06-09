/*
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
*/
var xRoll=function(el,fn){xRoll.prototype.init(el,fn)};xRoll.prototype={init:function(_el,fn){_el.attr("data-state",false);this.start(_el,fn);$(window).on("scroll",function(){xRoll.prototype.start(_el,fn)})},start:function(_el,fn){var _this=this;$(_el).each(function(){var _self=$(this);var xRollTop=$(window).scrollTop();var isWindowHeiget=$(window).height();if(_self.data().state){return}if(xRollTop+isWindowHeiget>$(this).offset().top){fn();setTimeout(function(){_self.attr("data-state",true);_self.data().state=true})}})}};