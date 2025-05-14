# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require 'java'

class DelayEvent
   include java.util.concurrent.Delayed

   attr_accessor :start_time, :event

   def initialize(delay, event)
      @start_time = Time.now.to_i + delay
      @event = event # Hash[table, TableEvents]
   end

   def get_delay(unit)
      delay = @start_time - Time.now.to_i
      unit.convert(delay, java.util.concurrent.TimeUnit::SECONDS)
   end

   def compare_to(other)
      d = self.start_time - other.start_time
      return 0 if d == 0
      d < 0 ? -1 : 1
   end

   def first_retry
      @event.values[0].req_count == 2
   end
end
