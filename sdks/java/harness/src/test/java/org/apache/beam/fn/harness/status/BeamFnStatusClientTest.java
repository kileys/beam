/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.fn.harness.status;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessorCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc.BeamFnWorkerStatusImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BeamFnStatusClientTest {
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor =
      Endpoints.ApiServiceDescriptor.newBuilder()
          .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
          .build();

  @Test
  public void testActiveBundleState() {
    ProcessBundleHandler handler = mock(ProcessBundleHandler.class);
    BundleProcessorCache processorCache = mock(BundleProcessorCache.class);
    Map<String, BundleProcessor> bundleProcessorMap = new HashMap<>();
    for (int i = 0; i < 11; i++) {
      BundleProcessor processor = mock(BundleProcessor.class);
      ExecutionStateTracker executionStateTracker = mock(ExecutionStateTracker.class);
      when(processor.getStateTracker()).thenReturn(executionStateTracker);
      when(executionStateTracker.getMillisSinceLastTransition())
          .thenReturn(Integer.toUnsignedLong((10 - i) * 1000));
      when(executionStateTracker.getTrackedThread()).thenReturn(Thread.currentThread());
      bundleProcessorMap.put(Integer.toString(i), processor);
    }
    when(handler.getBundleProcessorCache()).thenReturn(processorCache);
    when(processorCache.getActiveBundleProcessors()).thenReturn(bundleProcessorMap);

    ManagedChannelFactory channelFactory = InProcessManagedChannelFactory.create();
    BeamFnStatusClient client =
        new BeamFnStatusClient(
            apiServiceDescriptor,
            channelFactory::forDescriptor,
            OutboundObserverFactory.trivial(),
            handler.getBundleProcessorCache());
    StringJoiner joiner = new StringJoiner("\n");
    client.activeProcessBundleState(joiner);
    String actualState = joiner.toString();

    List<String> expectedInstructions = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      expectedInstructions.add(String.format("Instruction %d", i));
    }
    assertThat(actualState, stringContainsInOrder(expectedInstructions));
    assertThat(actualState, not(containsString("Instruction 10")));
  }

  @Test
  public void testWorkerStatusResponse() throws Exception {
    CountDownLatch requestCompleted = new CountDownLatch(1);
    BlockingQueue<WorkerStatusResponse> values = new LinkedBlockingQueue<>();
    AtomicReference<StreamObserver<WorkerStatusRequest>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<WorkerStatusResponse> inboundServerObserver =
        TestStreams.withOnNext(values::add)
            .withOnCompleted(
                () -> {
                  requestCompleted.countDown();
                  outboundServerObserver.get().onCompleted();
                })
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnWorkerStatusImplBase() {
                  @Override
                  public StreamObserver<WorkerStatusResponse> workerStatus(
                      StreamObserver<WorkerStatusRequest> responseObserver) {
                    outboundServerObserver.set(responseObserver);
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();

    try {
      ProcessBundleHandler bundleHandler =
          new ProcessBundleHandler(PipelineOptionsFactory.create(), null, null, null, null);
      ManagedChannelFactory channelFactory = InProcessManagedChannelFactory.create();
      BeamFnStatusClient client =
          new BeamFnStatusClient(
              apiServiceDescriptor,
              channelFactory::forDescriptor,
              OutboundObserverFactory.trivial(),
              bundleHandler.getBundleProcessorCache());
      BeamFnWorkerStatusGrpc.BeamFnWorkerStatusStub stub =
          BeamFnWorkerStatusGrpc.newStub(channelFactory.forDescriptor(apiServiceDescriptor));
      stub.workerStatus(outboundServerObserver.get());
      outboundServerObserver.get().onNext(WorkerStatusRequest.newBuilder().setId("id").build());
      outboundServerObserver.get().onCompleted();
      requestCompleted.await(5, TimeUnit.SECONDS);
      WorkerStatusResponse response = values.take();
      assertThat(response.getStatusInfo(), containsString("No active processing bundles."));
      assertThat(response.getId(), is("id"));
    } finally {
      server.shutdownNow();
    }
  }
}
