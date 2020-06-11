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
require 'resolv'
require 'mini_cache'

class ShortNameResolver
  def initialize(ttl:, logger:)
    @ttl = ttl
    @store = MiniCache::Store.new
    @logger = logger
  end

  private
  def resolve_cached(shortname)
    @store.get_or_set(shortname) do
      addresses = resolve(shortname)
      raise "Bad shortname '#{shortname}'" if addresses.empty?
      MiniCache::Data.new(addresses, expires_in: @ttl)
    end
  end

  private
  def resolve(shortname)
    addresses = Resolv::DNS.open do |dns|
      dns.getaddresses(shortname).map { |r| r.to_s }
    end

    @logger.info("Resolved shortname '#{shortname}' to addresses #{addresses}")

    return addresses
  end

  public
  def get_address(shortname)
    return resolve_cached(shortname).sample
  end

  public
  def get_addresses(shortname)
    return resolve_cached(shortname)
  end
end
