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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessorCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamFnStatusClient {
  private final StreamObserver<WorkerStatusResponse> outboundObserver;
  private final BundleProcessorCache processBundleCache;
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnStatusClient.class);

  public BeamFnStatusClient(
      ApiServiceDescriptor apiServiceDescriptor,
      Function<ApiServiceDescriptor, ManagedChannel> channelFactory,
      OutboundObserverFactory outboundObserverFactory,
      BundleProcessorCache processBundleCache) {
    BeamFnWorkerStatusGrpc.BeamFnWorkerStatusStub stub =
        BeamFnWorkerStatusGrpc.newStub(channelFactory.apply(apiServiceDescriptor));
    this.outboundObserver =
        outboundObserverFactory.outboundObserverFor(stub::workerStatus, new InboundObserver());
    this.processBundleCache = processBundleCache;
  }

  void threadDump(StringJoiner trace) {
    trace.add("========== THREAD DUMP ==========");
    Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
    threads
        .entrySet()
        .forEach(
            entry -> {
              trace.add(
                  String.format(
                      "%n--- Thread #%d name: %s ---",
                      entry.getKey().getId(), entry.getKey().getName()));
              Arrays.stream(entry.getValue()).map(StackTraceElement::toString).forEach(trace::add);
            });
  }

  @VisibleForTesting
  void activeProcessBundleState(StringJoiner activeBundlesState) {
    activeBundlesState.add("========== ACTIVE PROCESSING BUNDLES ==========");
    if (processBundleCache.getActiveBundleProcessors().isEmpty()) {
      activeBundlesState.add("No active processing bundles.\n");
    } else {
      processBundleCache.getActiveBundleProcessors().entrySet().stream()
          .sorted(
              Comparator.comparingLong(
                      (Map.Entry<String, BundleProcessor> bundle) ->
                          bundle.getValue().getStateTracker().getMillisSinceLastTransition())
                  .reversed()) // reverse sort active bundle by time since last transition.
          .limit(10) // only keep top 10
          .forEach(
              entry -> {
                ExecutionStateTracker executionStateTracker = entry.getValue().getStateTracker();
                activeBundlesState.add(String.format("---- Instruction %s ----", entry.getKey()));
                activeBundlesState.add(
                    String.format(
                        "Tracked thread: %s", executionStateTracker.getTrackedThread().getName()));
                activeBundlesState.add(
                    String.format(
                        "Time since transition: %.2f seconds%n",
                        executionStateTracker.getMillisSinceLastTransition() / 1000.0));
              });
    }
  }

  private class InboundObserver implements StreamObserver<BeamFnApi.WorkerStatusRequest> {
    @Override
    public void onNext(WorkerStatusRequest workerStatusRequest) {
      StringJoiner status = new StringJoiner("\n");
      activeProcessBundleState(status);
      status.add("\n");
      threadDump(status);
      outboundObserver.onNext(
          WorkerStatusResponse.newBuilder()
              .setId(workerStatusRequest.getId())
              .setStatusInfo(status.toString())
              .build());
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("Error getting SDK harness status", t);
    }

    @Override
    public void onCompleted() {}
  }
}
