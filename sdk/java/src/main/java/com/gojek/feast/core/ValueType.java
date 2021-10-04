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
package com.gojek.feast.core;

import feast.proto.types.ValueProto;
import java.util.Arrays;

public enum ValueType {
  INVALID(0),
  BYTES(1),
  STRING(2),
  INT32(3),
  INT64(4),
  DOUBLE(5),
  FLOAT(6),
  BOOL(7),
  UNIX_TIMESTAMP(8),
  BYTES_LIST(11),
  STRING_LIST(12),
  INT32_LIST(13),
  INT64_LIST(14),
  DOUBLE_LIST(15),
  FLOAT_LIST(16),
  BOOL_LIST(17),
  UNIX_TIMESTAMP_LIST(18);

  /** @param number - is the matching {@link feast.proto.types.ValueProto.Value} enumeration */
  ValueType(int number) {
    this.number = number;
  }

  private final int number;

  static ValueType fromProto(ValueProto.ValueType.Enum valueType) {
    return Arrays.stream(values())
        .filter(value -> value.number == valueType.getNumber())
        .findFirst()
        .orElse(INVALID);
  }

  ValueProto.ValueType.Enum toProto() {
    return ValueProto.ValueType.Enum.forNumber(number);
  }

  int getNumber() {
    return this.number;
  }
}
