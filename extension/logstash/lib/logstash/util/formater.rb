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

class Formater

  # copied form https://www.rubydoc.info/gems/logstash-event/LogStash/Event#sprintf-instance_method
  # modified by doris: return empty str if template field does not exist in the event rather than the template.
  def self.sprintf(event, format)
    format = format.to_s
    if format.index("%").nil?
      return format
    end

    format.gsub(/%\{[^}]+}/) do |tok|
      # Take the inside of the %{ ... }
      key = tok[2...-1]

      if key == "+%s"
        # Got %{+%s}, support for unix epoch time
        next event.timestamp.to_i
      elsif key[0, 1] == "+"
        t = event.timestamp
        formatter = org.joda.time.format.DateTimeFormat.forPattern(key[1..-1]) \
                       .withZone(org.joda.time.DateTimeZone::UTC)
        # next org.joda.time.Instant.new(t.tv_sec * 1000 + t.tv_usec / 1000).toDateTime.toString(formatter)
        # Invoke a specific Instant constructor to avoid this warning in JRuby
        #  > ambiguous Java methods found, using org.joda.time.Instant(long)
        # org.joda.time.Instant.java_class.constructor(Java::long).new_instance(
        #   t.tv_sec * 1000 + t.tv_usec / 1000
        # ).to_java.toDateTime.toString(formatter)
        mill = java.lang.Long.valueOf(t.to_i * 1000 + t.tv_usec / 1000)
        org.joda.time.Instant.new(mill).toDateTime.toString(formatter)
      else
        value = event.get(key)
        case value
        when nil
          '' # empty str if this field does not exist in this event
        when Array
          value.join(",") # join by ',' if value is an array
        when Hash
          value.to_json # convert hashes to json
        else
          value # otherwise return the value
        end # case value
      end # 'key' checking
    end # format.gsub...
  end

end
