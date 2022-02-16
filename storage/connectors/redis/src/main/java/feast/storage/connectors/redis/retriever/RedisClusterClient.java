/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.storage.connectors.redis.retriever;

import io.lettuce.core.*;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DnsResolvers;
import io.lettuce.core.resource.MappingSocketAddressResolver;
import io.lettuce.core.resource.NettyCustomizer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.epoll.EpollChannelOption;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisClusterClient implements RedisClientAdapter {

  private final RedisAdvancedClusterAsyncCommands<byte[], byte[]> asyncCommands;

  @Override
  public RedisFuture<List<KeyValue<byte[], byte[]>>> hmget(byte[] key, byte[]... fields) {
    return asyncCommands.hmget(key, fields);
  }

  @Override
  public void flushCommands() {
    asyncCommands.flushCommands();
  }

  static class Builder {
    private final StatefulRedisClusterConnection<byte[], byte[]> connection;

    Builder(StatefulRedisClusterConnection<byte[], byte[]> connection) {
      this.connection = connection;
    }

    RedisClusterClient build() {
      return new RedisClusterClient(this);
    }
  }

  private RedisClusterClient(Builder builder) {
    this.asyncCommands = builder.connection.async();

    // allows reading from replicas
    this.asyncCommands.readOnly();

    // Disable auto-flushing
    this.asyncCommands.setAutoFlushCommands(false);
  }

  public static String getAddressString(String host) {
    try {
      return InetAddress.getByName(host).getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(String.format("getAllByName() failed: %s", e.getMessage()));
    }
  }

  public static MappingSocketAddressResolver customSocketAddressResolver(
      RedisClusterStoreConfig config) {

    List<String> configuredHosts =
        Arrays.stream(config.getConnectionString().split(","))
            .map(
                hostPort -> {
                  return hostPort.trim().split(":")[0];
                })
            .collect(Collectors.toList());

    Map<String, String> mapAddressHost =
        configuredHosts.stream()
            .collect(
                Collectors.toMap(host -> ((String) getAddressString(host)), host -> (String) host));

    return MappingSocketAddressResolver.create(
        DnsResolvers.UNRESOLVED,
        hostAndPort ->
            mapAddressHost.keySet().stream().anyMatch(i -> i.equals(hostAndPort.getHostText()))
                ? hostAndPort.of(
                    mapAddressHost.get(hostAndPort.getHostText()), hostAndPort.getPort())
                : hostAndPort);
  }

  public static ClientResources customClientResources(RedisClusterStoreConfig config) {
    ClientResources clientResources =
        ClientResources.builder()
            .nettyCustomizer(
                new NettyCustomizer() {
                  @Override
                  public void afterBootstrapInitialized(Bootstrap bootstrap) {
                    bootstrap.option(EpollChannelOption.TCP_KEEPIDLE, 15);
                    bootstrap.option(EpollChannelOption.TCP_KEEPINTVL, 5);
                    bootstrap.option(EpollChannelOption.TCP_KEEPCNT, 3);
                    // Socket Timeout (milliseconds)
                    bootstrap.option(EpollChannelOption.TCP_USER_TIMEOUT, 60000);
                  }
                })
            .socketAddressResolver(customSocketAddressResolver(config))
            .build();
    return clientResources;
  }

  public static RedisClientAdapter create(RedisClusterStoreConfig config) {

    List<RedisURI> redisURIList =
        Arrays.stream(config.getConnectionString().split(","))
            .map(
                hostPort -> {
                  String[] hostPortSplit = hostPort.trim().split(":");
                  RedisURI redisURI =
                      RedisURI.create(hostPortSplit[0], Integer.parseInt(hostPortSplit[1]));
                  if (!config.getPassword().isEmpty()) {
                    redisURI.setPassword(config.getPassword());
                  }
                  if (config.getSsl()) {
                    redisURI.setSsl(true);
                  }
                  return redisURI;
                })
            .collect(Collectors.toList());

    io.lettuce.core.cluster.RedisClusterClient client =
        io.lettuce.core.cluster.RedisClusterClient.create(
            customClientResources(config), redisURIList);

    client.setOptions(
        ClusterClientOptions.builder()
            .socketOptions(SocketOptions.builder().keepAlive(true).tcpNoDelay(true).build())
            .timeoutOptions(TimeoutOptions.enabled(config.getTimeout()))
            .pingBeforeActivateConnection(true)
            .topologyRefreshOptions(
                ClusterTopologyRefreshOptions.builder().enableAllAdaptiveRefreshTriggers().build())
            .build());

    StatefulRedisClusterConnection<byte[], byte[]> connection =
        client.connect(new ByteArrayCodec());
    connection.setReadFrom(config.getReadFrom());

    return new Builder(connection).build();
  }
}
