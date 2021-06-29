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
package org.apache.beam.fn.harness.state;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MultimapUserStateTest {

  private final String pTransformId = "pTransformId";
  private final String stateId = "stateId";
  private final String encodedKey = "encodedKey";
  private final String encodedWindow = "encodedWindow";

  @Test
  public void testNoPersistedValues() throws Exception {
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(Collections.emptyMap());
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    assertTrue(Iterables.isEmpty(userState.keys()));
  }

  @Test
  public void testGet() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get("A1");
    userState.put("A1", "V3");
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertArrayEquals(
        new String[] {"V1", "V2", "V3"}, Iterables.toArray(userState.get("A1"), String.class));
    assertArrayEquals(new String[] {}, Iterables.toArray(userState.get("A2"), String.class));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.get("A1"));
  }

  @Test
  public void testClear() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get("A1");
    userState.clear();
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertTrue(Iterables.isEmpty(userState.get("A1")));
    assertTrue(Iterables.isEmpty(userState.keys()));

    userState.put("A1", "V1");
    userState.clear();
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertTrue(Iterables.isEmpty(userState.get("A1")));
    assertTrue(Iterables.isEmpty(userState.keys()));

    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.clear());
  }

  @Test
  public void testKeys() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());

    userState.put("A2", "V1");
    Iterable<String> initKeys = userState.keys();
    userState.put("A3", "V1");
    userState.put("A1", "V3");
    assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(initKeys, String.class));
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"}, Iterables.toArray(userState.keys(), String.class));

    userState.clear();
    assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(initKeys, String.class));
    assertArrayEquals(new String[] {}, Iterables.toArray(userState.keys(), String.class));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.keys());
  }

  @Test
  public void testPut() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get("A1");
    userState.put("A1", "V3");
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertArrayEquals(
        new String[] {"V1", "V2", "V3"}, Iterables.toArray(userState.get("A1"), String.class));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.put("A1", "V2"));
  }

  @Test
  public void testRemove() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());

    Iterable<String> initValues = userState.get("A1");
    userState.put("A1", "V3");

    userState.remove("A1");
    assertArrayEquals(new String[] {"V1", "V2"}, Iterables.toArray(initValues, String.class));
    assertTrue(Iterables.isEmpty(userState.keys()));
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.remove("A1"));
  }

  @Test
  public void testImmutableKeys() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    Iterable<String> keys = userState.keys();
    assertThrows(
        UnsupportedOperationException.class, () -> Iterables.removeAll(keys, Arrays.asList("A1")));
  }

  @Test
  public void testImmutableValues() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    Iterable<String> values = userState.get("A1");
    assertThrows(
        UnsupportedOperationException.class,
        () -> Iterables.removeAll(values, Arrays.asList("V1")));
  }

  @Test
  public void testClearAsyncClose() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    userState.clear();
    userState.asyncClose();
    Map<StateKey, ByteString> data = fakeClient.getData();
    assertEquals(1, data.size());
    assertNull(data.get(createMultimapKeyStateKey()));
  }

  @Test
  public void testNoopAsyncClose() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    userState.asyncClose();
    assertThrows(IllegalStateException.class, () -> userState.keys());
    assertEquals(0, fakeClient.getCallCount());
  }

  @Test
  public void testAsyncClose() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                createMultimapKeyStateKey(),
                encode("A0", "A1"),
                createMultimapValueStateKey("A0"),
                encode("V1"),
                createMultimapValueStateKey("A1"),
                encode("V1", "V2")));
    MultimapUserState<String, String> userState =
        new MultimapUserState<>(
            fakeClient,
            "instructionId",
            pTransformId,
            stateId,
            encode(encodedWindow),
            encode(encodedKey),
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    userState.remove("A0");
    userState.put("A1", "V3");
    userState.put("A2", "V1");
    userState.put("A3", "V1");
    userState.remove("A3");
    userState.asyncClose();
    Map<StateKey, ByteString> data = fakeClient.getData();
    assertNull(data.get(createMultimapValueStateKey("A0")));
    assertEquals(encode("V1", "V2", "V3"), data.get(createMultimapValueStateKey("A1")));
    assertEquals(encode("V1"), data.get(createMultimapValueStateKey("A2")));
  }

  private StateKey createMultimapKeyStateKey() throws IOException {
    return StateKey.newBuilder()
        .setMultimapKeysUserState(
            StateKey.MultimapKeysUserState.newBuilder()
                .setWindow(encode(encodedWindow))
                .setKey(encode(encodedKey))
                .setTransformId(pTransformId)
                .setUserStateId(stateId))
        .build();
  }

  private StateKey createMultimapValueStateKey(String key) throws IOException {
    return StateKey.newBuilder()
        .setMultimapUserState(
            StateKey.MultimapUserState.newBuilder()
                .setTransformId(pTransformId)
                .setUserStateId(stateId)
                .setWindow(encode(encodedWindow))
                .setKey(encode(encodedKey))
                .setMapKey(encode(key)))
        .build();
  }

  private ByteString encode(String... values) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    for (String value : values) {
      StringUtf8Coder.of().encode(value, out);
    }
    return out.toByteString();
  }
}
