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
require "stud/buffer"
require "logstash/plugin_mixins/http_client"
require "securerandom"
require "json"
require "base64"
require "restclient"


class LogStash::Outputs::Doris < LogStash::Outputs::Base
   include LogStash::PluginMixins::HttpClient
   include Stud::Buffer

   concurrency :single

   config_name "doris"
   # hosts array of Doris Frontends. eg ["http://fe1:8030", "http://fe2:8030"]
   config :http_hosts, :validate => :array, :required => true
   # the database which data is loaded to
   config :db, :validate => :string, :required => true
   # the table which data is loaded to
   config :table, :validate => :string, :required => true
   # label prefix of a stream load requst.
   config :label_prefix, :validate => :string, :required => true
   # user
   config :user, :validate => :string, :required => true
   # password
   config :password, :validate => :password, :required => true
   # column separator
   config :column_separator, :validate => :string, :default => ""
   # column mappings. eg: "k1, k2, tmpk3, k3 = tmpk3 + 1"
   config :columns, :validate => :string, :default => ""
   # where predicate to filter data. eg: "k1 > 1 and k3 < 100"
   config :where, :validate => :string, :default => ""
   # max filter ratio
   config :max_filter_ratio, :validate => :number, :default => -1
   # partition which data is loaded to. eg: "p1, p2"
   config :partition, :validate => :array, :default => {}
   # timeout of a stream load, in second
   config :timeout, :validate => :number, :default => -1
   # switch off or on of strict mode
   config :strict_mode, :validate => :string, :default => "false"
   # timezone
   config :timezone, :validate => :string, :default => ""
   # memory limit of a stream load
   config :exec_mem_limit, :validate => :number, :default => -1

   # Custom headers to use
   # format is `headers => ["X-My-Header", "%{host}"]`
   config :headers, :validate => :hash

   config :batch_size, :validate => :number, :default => 100000

   config :idle_flush_time, :validate => :number, :default => 20

   config :save_on_failure, :validate => :boolean, :default => true

   config :save_dir, :validate => :string, :default => "/tmp"

   config :save_file, :validate => :string, :default => "failed.data"

   config :host_resolve_ttl_sec, :validate => :number, :default => 120

   config :automatic_retries, :validate => :number, :default => 3


   def print_plugin_info()
      @@plugins = Gem::Specification.find_all{|spec| spec.name =~ /logstash-output-doris/ }
      @plugin_name = @@plugins[0].name
      @plugin_version = @@plugins[0].version
      @logger.debug("Running #{@plugin_name} version #{@plugin_version}")

      @logger.info("Initialized doris output with settings",
      :db => @db,
      :table => @table,
      :label_prefix => @label_prefix,
      :batch_size => @batch_size,
      :idle_flush_time => @idle_flush_time,
      :http_hosts => @http_hosts)
   end

   def register
      # Handle this deprecated option. TODO: remove the option
      #@ssl_certificate_validation = @verify_ssl if @verify_ssl

      # We count outstanding requests with this queue
      # This queue tracks the requests to create backpressure
      # When this queue is empty no new requests may be sent,
      # tokens must be added back by the client on success
      #@request_tokens = SizedQueue.new(@pool_max)
      #@pool_max.times {|t| @request_tokens << true }
      #@requests = Array.new

      @http_query = "/api/#{db}/#{table}/_stream_load"

      @hostnames_pool =
      parse_http_hosts(http_hosts,
      ShortNameResolver.new(ttl: @host_resolve_ttl_sec, logger: @logger))

      @request_headers = make_request_headers
      @logger.info("request headers: ", @request_headers)

      buffer_initialize(
      :max_items => @batch_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
      )

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

   # This module currently does not support parallel requests as that would circumvent the batching
   def receive(event)
      buffer_receive(event)
   end

   public
   def flush(events, close=false)
      documents = ""
      event_num = 0
      events.each do |event|
         documents << event.get("[message]") << "\n"
         event_num += 1
      end

      @logger.info("get event num: #{event_num}")
      @logger.debug("get documents: #{documents}")

      hosts = get_host_addresses()

      @request_headers["label"] = label_prefix + "_" + @db + "_" + @table + "_" + Time.now.strftime('%Y%m%d%H%M%S_%L')
      make_request(documents, hosts, @http_query, 1, hosts.sample)
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


   private

   def make_request(documents, hosts, query, req_count = 1,host = "", uuid = SecureRandom.hex)

      if host == ""
         host = hosts.pop
      end

      url = host+query
      @logger.debug("req count: #{req_count}. get url: #{url}")
      @logger.debug("request headers: ", @request_headers)
      

      result = RestClient.put(url, documents,@request_headers) { |response, request, result|
                case response.code
                when 301, 302, 307
                    @logger.debug("redirect to: #{response.headers[:location]}")
                    response.follow_redirection
                else
                    response.return!
                end
            }

      @logger.info("response : \n #{result}" )
      result_body = JSON.parse(result.body)
      if result_body['Status'] != "Success"
         if req_count < @automatic_retries
            @logger.warn("Response Status : #{result_body['Status']} . Retrying...... #{req_count}")
            make_request(documents,hosts,query,req_count + 1,host,uuid)
            return
         end
         @logger.warn("Load failed ! Try #{req_count} times.")
         if @save_on_failure
            @logger.warn("Retry times over #{req_count} times.Try save to disk.Disk file path : #{save_dir}/#{table}_#{save_file}")
            save_to_disk(documents)
         end
      end

   end # def make_request

    # This is split into a separate method mostly to help testing
   def log_failure(message, opts)
      @logger.warn("[HTTP Output Failure] #{message}", opts)
   end

   def make_request_headers()
      headers = @headers || {}
      headers["Expect"] ||= "100-continue"
      headers["Content-Type"] ||= "text/plain;charset=utf-8"
      headers["strict_mode"] ||= @strict_mode
      headers["Authorization"] = "Basic " + Base64.strict_encode64("#{user}:#{password.value}")
      # column_separator
      if @column_separator != ""
         headers["column_separator"] = @column_separator
      end
      # timezone
      if @timezone != ""
           headers["timezone"] = @timezone
      end
      # partition
      if @partition.size > 0
           headers["partition"] ||= @partition
      end
      # where
      if @where != ""
           headers["where"] ||= @where
      end
      # timeout
      if @timeout != -1
           headers["timeout"] ||= @timeout
      end
      # max_filter_ratio
      if @max_filter_ratio != -1
           headers["max_filter_ratio"] ||= @max_filter_ratio
      end
      # exec_mem_limit
      if @exec_mem_limit != -1
           headers["exec_mem_limit"] ||= @exec_mem_limit
      end
      # columns
      if @columns != ""
          headers["columns"] ||= @columns
      end
      headers
   end
end # end of class LogStash::Outputs::Doris


