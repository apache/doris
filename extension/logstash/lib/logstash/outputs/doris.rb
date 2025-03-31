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
require 'logstash/util/formater'
require 'logstash/util/delay_event'
require "uri"
require "securerandom"
require "json"
require "base64"
require 'thread'

require 'java'
require "#{File.dirname(__FILE__)}/../../logstash-output-doris_jars.rb"

class LogStash::Outputs::Doris < LogStash::Outputs::Base
   include_package 'org.apache.hc.client5.http.impl.async'
   include_package 'org.apache.hc.client5.http.async.methods'
   include_package 'org.apache.hc.core5.http'

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
   # default table
   config :default_table, :validate => :string, :default => ""
   # label prefix of a stream load request.
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

   config :max_retries, :validate => :number, :default => -1

   config :log_request, :validate => :boolean, :default => true

   config :log_progress_interval, :validate => :number, :default => 10

   # max retry queue size in MB, default is 20% max memory of JVM
   config :max_retry_queue_mb, :validate => :number, :default => java.lang.Runtime.get_runtime.max_memory / 1024 / 1024 / 5

   def print_plugin_info()
      @plugins = Gem::Specification.find_all{|spec| spec.name =~ /logstash-output-doris/ }
      @plugin_name = @plugins[0].name
      @plugin_version = @plugins[0].version
      @logger.debug("Running #{@plugin_name} version #{@plugin_version}")

      @logger.info("Initialized doris output with settings",
      :db => @db,
      :table => @table,
      :label_prefix => @label_prefix,
      :http_hosts => @http_hosts)
   end

   class DorisRedirectStrategy < Java::org.apache.hc.client5.http.impl.DefaultRedirectStrategy
      def getLocationURI(request, response, context)
         uri = super(request, response, context)
         # remove user info in redirect uri
         java.net.URI.new(uri.getScheme, nil, uri.getHost, uri.getPort, uri.getPath, uri.getQuery, uri.getFragment)
      end
   end

   def http_query(table)
      "/api/#{@db}/#{table}/_stream_load"
   end

   def register
      @client = HttpAsyncClients.custom.setRedirectStrategy(DorisRedirectStrategy.new).build
      @client.start

      @request_headers = make_request_headers
      @logger.info("request headers: ", @request_headers)

      @group_commit = false
      if @request_headers.has_key?("group_commit") && @request_headers["group_commit"] != "off_mode"
         @group_commit = true
      end
      @logger.info("group_commit: ", @group_commit)

      @init_time = Time.now.to_i # seconds
      @total_bytes = java.util.concurrent.atomic.AtomicLong.new(0)
      @total_rows = java.util.concurrent.atomic.AtomicLong.new(0)

      report_thread = Thread.new do
         last_time = @init_time
         last_bytes = @total_bytes.get
         last_rows = @total_rows.get
         @logger.info("will report speed every #{@log_progress_interval} seconds")
         while @log_progress_interval > 0
            sleep(@log_progress_interval)

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

      if @max_retry_queue_mb <= 0
         @max_retry_queue_mb = java.lang.Runtime.get_runtime.max_memory / 1024 / 1024 / 5
      end
      @logger.info("max retry queue size: #{@max_retry_queue_mb}MB")

      @retry_queue = java.util.concurrent.DelayQueue.new
      # retry queue size in bytes
      @retry_queue_bytes = java.util.concurrent.atomic.AtomicLong.new(0)
      retry_thread = Thread.new do
         while (popped = @retry_queue.take)
            table_events_map = popped.event
            handle_request(table_events_map)
         end
      end

      @const_table = @table.index("%").nil?

      print_plugin_info
   end # def register

   private
   def add_event_to_retry_queue(delay_event)
      event_size = 0
      delay_event.event.each do |_, table_events|
         event_size += table_events.documents.size
      end

      if delay_event.first_retry
         while @retry_queue_bytes.get + event_size > @max_retry_queue_mb * 1024 * 1024
            sleep(1)
         end
         @retry_queue_bytes.addAndGet(event_size)
      end
      @retry_queue.add(delay_event)
   end

   def multi_receive(events)
      return if events.empty?
      send_events(events)
   end

   def create_http_headers(table)
      http_headers = @request_headers.dup
      if !@group_commit
         # only set label if group_commit is off_mode or not set, since lable can not be used with group_commit
         http_headers["label"] = @label_prefix + "_" + @db + "_" + table + "_" + Time.now.strftime('%Y%m%d_%H%M%S_%L_' + SecureRandom.uuid)
      end
      http_headers
   end

   private
   def send_events(events)
      table_events_map = Hash.new
      if @const_table
         table_events = TableEvents.new(@table, create_http_headers(@table))
         table_events.events = events
         table_events_map[@table] = table_events
      else
         events.each do |event|
            table = event.sprintf(@table)
            if table == "" || !table.index("%").nil?
               table = @default_table
               if table == ""
                  @logger.warn("table format error, the default table is not set, the data will be dropped")
               else
                  @logger.warn("table format error, use the default table: #{table}")
               end
            end
            table_events = table_events_map[table]
            if table_events == nil
               table_events = TableEvents.new(table, create_http_headers(table))
               table_events_map[table] = table_events
            end
            table_events.events << event
         end
      end

      table_events_map.each do |_, table_events|
         serialize(table_events)
      end

      handle_request(table_events_map)
   end

   def sleep_for_attempt(attempt)
      sleep_for = attempt**2
      sleep_for = sleep_for <= 60 ? sleep_for : 60
      (sleep_for/2) + (rand(0..sleep_for)/2)
   end

   STAT_SUCCESS = 0
   STAT_FAIL = 1
   STAT_RETRY = 2

   private
   def handle_request(table_events_map)
      make_request(table_events_map)
      retry_map = Hash.new
      table_events_map.each do |table, table_events|
         stat = STAT_SUCCESS

         if table == ""
            @logger.warn("drop #{table_events.events_count} records because of empty table")
            stat = STAT_FAIL
         end

         response = ""
         if stat == STAT_SUCCESS
            begin
               response = table_events.response_future.get.getBodyText
            rescue => e
               log_failure("doris stream load request error: #{e}")
               stat = STAT_RETRY
            end
         end

         response_json = {}
         if stat == STAT_SUCCESS
            begin
               response_json = JSON.parse(response)
            rescue => _
               @logger.warn("doris stream load response is not a valid JSON:\n#{response}")
               stat = STAT_RETRY
            end
         end

         if stat == STAT_SUCCESS
            status = response_json["Status"]

            if status == 'Label Already Exists'
               @logger.warn("Label already exists: #{response_json['Label']}, skip #{table_events.events_count} records:\n#{response}")

            elsif status == "Success" || status == "Publish Timeout"
               @total_bytes.addAndGet(table_events.documents.size)
               @total_rows.addAndGet(table_events.events_count)
               if @log_request or @logger.debug?
                  @logger.info("doris stream load response:\n#{response}")
               end

            else
               @logger.warn("FAILED doris stream load response:\n#{response}")
               if @max_retries >= 0 && table_events.req_count - 1 >= @max_retries
                  @logger.warn("DROP this batch after failed #{table_events.req_count} times.")
                  stat = STAT_FAIL
               else
                  stat = STAT_RETRY
               end
            end
         end

         if stat == STAT_FAIL && @save_on_failure
            @logger.warn("Try save to disk.Disk file path : #{@save_dir}/#{table}_#{@save_file}")
            save_to_disk(table_events.documents, table)
         end

         if stat != STAT_RETRY && table_events.req_count > 1
            @retry_queue_bytes.addAndGet(-table_events.documents.size)
         end

         if stat == STAT_RETRY
            table_events.prepare_retry
            retry_map[table] = table_events
         end
      end

      if retry_map.size > 0
         # add to retry_queue
         req_count = retry_map.values[0].req_count
         sleep_for = sleep_for_attempt(req_count)
         @logger.warn("Will do the #{req_count-1}th retry after #{sleep_for} secs.")
         delay_event = DelayEvent.new(sleep_for, retry_map)
         add_event_to_retry_queue(delay_event)
      end
   end

   private
   def make_request(table_events_map)
      table_events_map.each do |table, table_events|
         url = @http_hosts.sample + http_query(table)

         if @log_request or @logger.debug?
            @logger.info("doris stream load request url: #{url}  headers: #{table_events.http_headers}  body size: #{table_events.documents.size}")
         end
         @logger.debug("doris stream load request body: #{table_events.documents}")

         request = SimpleRequestBuilder.
            put(url).
            setBody(table_events.documents, ContentType::TEXT_PLAIN).
            build
         table_events.http_headers.each do |k, v|
            request.addHeader(k, v)
         end

         table_events.response_future = @client.execute(request, nil)
      end
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
        Formater.sprintf(event, mapping)
      end
   end

   private
   def save_to_disk(documents, table)
      begin
         file = File.open("#{@save_dir}/#{@db}_#{table}_#{@save_file}", "a")
         file.write(documents, "\n")
      rescue IOError => e
         log_failure("An error occurred while saving file to disk: #{e}",
         :file_name => file_name)
      ensure
         file.close unless file.nil?
      end
   end

    # This is split into a separate method mostly to help testing
   def log_failure(message, data = {})
      @logger.warn("[Doris Output Failure] #{message}", data)
   end

   def make_request_headers()
      headers = @headers || {}
      headers["Expect"] ||= "100-continue"
      headers["Content-Type"] ||= "text/plain;charset=utf-8"
      headers["Authorization"] = "Basic " + Base64.strict_encode64("#{@user}:#{@password.value}")
  
      headers
   end

   def serialize(table_events)
      table_events.events_count = table_events.events.size
      table_events.documents = table_events.events.map { |e| event_body(e) }.join("\n")
      table_events.events = nil # no longer used, can be gc

      @logger.debug("get documents: #{table_events.documents}")
   end

   class TableEvents
      attr_accessor :table, :http_headers, :events, :events_count, :documents, :req_count, :response_future

      def initialize(table, http_headers)
         @table = table
         @http_headers = http_headers

         @events = []
         @events_count = 0
         @documents = ""
         @req_count = 1

         @response_future = nil
      end

      def prepare_retry
         @req_count += 1
         @response_future = nil
      end
   end

end # end of class LogStash::Outputs::Doris
