=begin
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
=end

# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"
require "logstash/util/shortname_resolver"
require "uri"
require "logstash/plugin_mixins/http_client"
require "securerandom"
require "json"
require "base64"
require "restclient"
require 'thread'


class LogStash::Outputs::Doris < LogStash::Outputs::Base
   # support multi thread concurrency for performance
   # so multi_receive() and function it calls are all stateless and thread safe
   concurrency :shared

   config_name "doris"

   # hosts array of Doris Frontends. eg ["http://fe1:8030", "http://fe2:8030"]
   config :http_hosts, :validate => :array, :required => true
   # the database which data is loaded to
   config :db, :validate => :string, :required => true
   # the table which data is loaded to
   config :table, :validate => :string, :required => true
   # label prefix of a stream load requst.
   config :label_prefix, :validate => :string, :default => "logstash"
   # user name
   config :user, :validate => :string, :required => true
   # password
   config :password, :validate => :password, :required => true

   # use message field only
   config :message_only, :validate => :boolean, :default => false
   # field mapping
   config :mapping, :validate => :hash


   # Custom headers to use
   # format is `headers => ["X-My-Header", "%{host}"]`
   config :headers, :validate => :hash

   config :save_on_failure, :validate => :boolean, :default => false

   config :save_dir, :validate => :string, :default => "./"

   config :save_file, :validate => :string, :default => "failed.data"

   config :host_resolve_ttl_sec, :validate => :number, :default => 120

   config :retry_count, :validate => :number, :default => -1

   config :log_request, :validate => :boolean, :default => false

   config :log_speed_interval, :validate => :number, :default => 10


   def print_plugin_info()
      @@plugins = Gem::Specification.find_all{|spec| spec.name =~ /logstash-output-doris/ }
      @plugin_name = @@plugins[0].name
      @plugin_version = @@plugins[0].version
      @logger.debug("Running #{@plugin_name} version #{@plugin_version}")

      @logger.info("Initialized doris output with settings",
      :db => @db,
      :table => @table,
      :label_prefix => @label_prefix,
      :http_hosts => @http_hosts)
   end

   def register
      @http_query = "/api/#{db}/#{table}/_stream_load"

      @hostnames_pool =
      parse_http_hosts(http_hosts,
      ShortNameResolver.new(ttl: @host_resolve_ttl_sec, logger: @logger))

      @request_headers = make_request_headers
      @logger.info("request headers: ", @request_headers)

      @init_time = Time.now.to_i # seconds
      @total_bytes = java.util.concurrent.atomic.AtomicLong.new(0)
      @total_rows = java.util.concurrent.atomic.AtomicLong.new(0)

      report_thread = Thread.new do
         last_time = @init_time
         last_bytes = @total_bytes.get
         last_rows = @total_rows.get
         @logger.info("will report speed every #{@log_speed_interval} seconds")
         while @log_speed_interval > 0
            sleep(@log_speed_interval)

            cur_time = Time.now.to_i # seconds
            cur_bytes = @total_bytes.get
            cur_rows = @total_rows.get
            total_time = cur_time - @init_time
            total_speed_mbps = cur_bytes / 1024 / 1024 / total_time
            total_speed_rps = cur_rows / total_time

            inc_bytes = cur_bytes - last_bytes
            inc_rows = cur_rows - last_rows
            inc_time = cur_time - last_time
            inc_speed_mbps = inc_bytes / 1024 / 1024 / inc_time
            inc_speed_rps = inc_rows / inc_time

            @logger.info("total #{cur_bytes/1024/1024} MB #{cur_rows} ROWS, total speed #{total_speed_mbps} MB/s #{total_speed_rps} R/s, last #{inc_time} seconds speed #{inc_speed_mbps} MB/s #{inc_speed_rps} R/s")

            last_time = cur_time
            last_bytes = cur_bytes
            last_rows = cur_rows
         end
      end

      print_plugin_info()
   end # def register

   private

   def parse_http_hosts(hosts, resolver)
      ip_re = /^[\d]+\.[\d]+\.[\d]+\.[\d]+$/

      lambda {
         hosts.flat_map { |h|
            scheme = URI(h).scheme
            host = URI(h).host
            port = URI(h).port
            path = URI(h).path

            if ip_re !~ host
               resolver.get_addresses(host).map { |ip|
                  "#{scheme}://#{ip}:#{port}#{path}"
               }
            else
               [h]
            end
         }
      }
   end

   private

   def get_host_addresses()
      begin
         @hostnames_pool.call
      rescue Exception => ex
         @logger.error('Error while resolving host', :error => ex.to_s)
      end
   end

   def multi_receive(events)
      return if events.empty?
      send_events(events)
   end

   private
   def send_events(events)
      documents = ""
      event_num = 0
      events.each do |event|
         documents << event_body(event) << "\n"
         event_num += 1
      end

      # @logger.info("get event num: #{event_num}")
      @logger.debug("get documents: #{documents}")

      hosts = get_host_addresses()

      http_headers = @request_headers.dup
      http_headers["label"] = label_prefix + "_" + @db + "_" + @table + "_" + Time.now.strftime('%Y%m%d_%H%M%S_%L_' + SecureRandom.uuid)

      # @request_headers["label"] = label_prefix + "_" + @db + "_" + @table + "_" + Time.now.strftime('%Y%m%d%H%M%S_%L')
      req_count = 0
      sleep_for = 1
      while true
         response = make_request(documents, http_headers, hosts, @http_query, hosts.sample)

         req_count += 1
         response_json = {}
         begin
            response_json = JSON.parse(response.body)
         rescue => e
            @logger.warn("doris stream load response: #{response} is not a valid JSON")
         end
         if response_json["Status"] == "Success"
            @total_bytes.addAndGet(documents.size)
            @total_rows.addAndGet(event_num)
            break
         else
            @logger.warn("FAILED doris stream load response:\n#{response}")

            if @retry_count >= 0 && req_count > @retry_count
               @logger.warn("DROP this batch after failed #{req_count} times.")
               if @save_on_failure
                  @logger.warn("Try save to disk.Disk file path : #{save_dir}/#{table}_#{save_file}")
                  save_to_disk(documents)
               end
               break
            end

            # sleep and then retry
            sleep_for = sleep_for * 2
            sleep_for = sleep_for <= 60 ? sleep_for : 60
            sleep_rand = (sleep_for / 2) + (rand(0..sleep_for) / 2)
            @logger.warn("Will do retry #{req_count} after sleep #{sleep_rand} secs.")
            sleep(sleep_rand)
         end
      end
   end

   private
   def make_request(documents, http_headers, hosts, query, host = "")
      if host == ""
         host = hosts.pop
      end

      url = host + query

      if @log_request or @logger.debug?
         @logger.info("doris stream load request url: #{url}  headers: #{http_headers}  body size: #{documents.size}")
      end
      @logger.debug("doris stream load request body: #{documents}")

      response = ""
      begin
         response = RestClient.put(url, documents, http_headers) { |response, request, result|
                case response.code
                when 301, 302, 307
                    @logger.debug("redirect to: #{response.headers[:location]}")
                    response.follow_redirection
                else
                    response.return!
                end
         }
      rescue => e
         log_failure("doris stream load request error: #{e}")
      end

      if @log_request or @logger.debug?
         @logger.info("doris stream load response:\n#{response}")
      end

      return response
   end # def make_request

   # Format the HTTP body
   private
   def event_body(event)
      if @message_only
         event.get("[message]")
      else
         LogStash::Json.dump(map_event(event))
      end
   end

   private
   def map_event(event)
      if @mapping
        # only get fields in mapping
        convert_mapping(@mapping, event)
      else
        # get all fields
        event.to_hash
      end
   end

   private
   def convert_mapping(mapping, event)
      if mapping.is_a?(Hash)
        mapping.reduce({}) do |acc, kv|
          k, v = kv
          acc[k] = convert_mapping(v, event)
          acc
        end
      elsif mapping.is_a?(Array)
        mapping.map { |elem| convert_mapping(elem, event) }
      else
        event.sprintf(mapping)
      end
   end

   private
   def save_to_disk(documents)
      begin
         file = File.open("#{save_dir}/#{db}_#{table}_#{save_file}", "a")
         file.write(documents)
      rescue IOError => e
         log_failure("An error occurred while saving file to disk: #{e}",
         :file_name => file_name)
      ensure
         file.close unless file.nil?
      end
   end

    # This is split into a separate method mostly to help testing
   def log_failure(message)
      @logger.warn("[Doris Output Failure] #{message}")
   end

   def make_request_headers()
      headers = @headers || {}
      headers["Expect"] ||= "100-continue"
      headers["Content-Type"] ||= "text/plain;charset=utf-8"
      headers["Authorization"] = "Basic " + Base64.strict_encode64("#{user}:#{password.value}")
  
      headers
   end
end # end of class LogStash::Outputs::Doris
