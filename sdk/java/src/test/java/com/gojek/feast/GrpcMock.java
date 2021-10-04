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

import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;

public class GrpcMock {

  @Rule public GrpcCleanupRule grpcRule;
  protected AtomicBoolean isAuthenticated;
  private ManagedChannel channel;

  public GrpcMock(BindableService server) throws IOException {
    this.grpcRule = new GrpcCleanupRule();
    this.isAuthenticated = new AtomicBoolean(false);
    // setup fake serving service
    String serverName = InProcessServerBuilder.generateName();
    this.grpcRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(server)
            .intercept(mockAuthInterceptor)
            .build()
            .start());
    // setup test feast client target
    this.channel =
        this.grpcRule.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  // Mock Authentication interceptor will flag authenticated request by setting isAuthenticated to
  // true.
  private final ServerInterceptor mockAuthInterceptor =
      new ServerInterceptor() {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
          final Metadata.Key<String> authorizationKey =
              Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);
          if (headers.containsKey(authorizationKey)) {
            isAuthenticated.set(true);
          }
          return next.startCall(call, headers);
        }
      };

  public ManagedChannel getChannel() {
    return channel;
  }

  public boolean isAuthenticated() {
    return isAuthenticated.get();
  }

  public void resetAuthentication() {
    isAuthenticated.set(false);
  }
}
