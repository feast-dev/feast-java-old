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
package feast.storage.connectors.redis.retriever;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ProtocolStringList;
import feast.proto.storage.RedisProto;
import feast.proto.types.ValueProto;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public class EntityKeySerializerV2 implements EntityKeySerializer {
  @Override
  public byte[] serialize(RedisProto.RedisKeyV2 entityKey) {
    final ProtocolStringList joinKeys = entityKey.getEntityNamesList();
    final List<ValueProto.Value> values = entityKey.getEntityValuesList();

    assert joinKeys.size() == values.size();

    final List<Byte> buffer = new ArrayList<>();

    final List<Pair<String, ValueProto.Value>> tuples = new ArrayList<>(joinKeys.size());
    for (int i = 0; i < joinKeys.size(); i++) {
      tuples.add(Pair.of(joinKeys.get(i), values.get(i)));
    }
    tuples.sort(Comparator.comparing(Pair::getLeft));
    for (Pair<String, ValueProto.Value> pair : tuples) {
      for (final byte b : pair.getLeft().getBytes(StandardCharsets.UTF_8)) {
        buffer.add(b);
      }
    }

    for (Pair<String, ValueProto.Value> pair : tuples) {
      final ValueProto.Value val = pair.getRight();
      switch (val.getValCase()) {
        case STRING_VAL:
          buffer.add(UnsignedBytes.checkedCast(ValueProto.ValueType.Enum.STRING.getNumber()));
          buffer.add(
              UnsignedBytes.checkedCast(
                  val.getStringVal().getBytes(StandardCharsets.UTF_8).length));
          for (final byte b : val.getStringVal().getBytes(StandardCharsets.UTF_8)) {
            buffer.add(b);
          }
          break;
        case BYTES_VAL:
        case INT32_VAL:
        case INT64_VAL:
          break;
        default:
          break;
      }
    }
    for (final byte b : entityKey.getProject().getBytes(StandardCharsets.UTF_8)) {
      buffer.add(b);
    }

    final byte[] bytes = new byte[buffer.size()];
    for (int i = 0; i < buffer.size(); i++) {
      bytes[i] = buffer.get(i);
    }

    return bytes;
  }
}
