/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2021 The Feast Authors
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
package com.gojek.feast;

import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.opentracing.contrib.grpc.TracingClientInterceptor;
import io.opentracing.util.GlobalTracer;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;

public abstract class GrpcManager<S extends io.grpc.stub.AbstractBlockingStub<S>>
    implements AutoCloseable {

  private static final int CHANNEL_SHUTDOWN_TIMEOUT_SEC = 5;

  private final ManagedChannel channel;
  protected final S stub;

  protected GrpcManager(String host, int port, SecurityConfig securityConfig) {
    this(createSecureChannel(host, port, securityConfig), securityConfig.getCredentials());
  }

  protected abstract S getStub(Channel channel);

  protected GrpcManager(ManagedChannel channel, Optional<CallCredentials> credentials) {
    this.channel = channel;
    TracingClientInterceptor tracingInterceptor =
        TracingClientInterceptor.newBuilder().withTracer(GlobalTracer.get()).build();

    S servingStub = getStub(tracingInterceptor.intercept(channel));

    if (credentials.isPresent()) {
      servingStub = servingStub.withCallCredentials(credentials.get());
    }

    this.stub = servingStub;
  }

  private static ManagedChannel createSecureChannel(
      String host, int port, SecurityConfig securityConfig) {
    ManagedChannel channel;
    if (securityConfig.isTLSEnabled()) {
      if (securityConfig.getCertificatePath().isPresent()) {
        String certificatePath = securityConfig.getCertificatePath().get();
        // Use custom certificate for TLS
        File certificateFile = new File(certificatePath);
        try {
          channel =
              NettyChannelBuilder.forAddress(host, port)
                  .useTransportSecurity()
                  .sslContext(GrpcSslContexts.forClient().trustManager(certificateFile).build())
                  .build();
        } catch (SSLException e) {
          throw new IllegalArgumentException(
              String.format("Invalid Certificate provided at path: %s", certificatePath), e);
        }
      } else {
        // Use system certificates for TLS
        channel = ManagedChannelBuilder.forAddress(host, port).useTransportSecurity().build();
      }
    } else {
      // Disable TLS
      channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }
    return channel;
  }

  @Override
  public void close() throws Exception {
    if (channel != null) {
      channel.shutdown().awaitTermination(CHANNEL_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }
  }
}
