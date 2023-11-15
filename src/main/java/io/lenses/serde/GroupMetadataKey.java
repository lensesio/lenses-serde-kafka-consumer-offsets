/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at: http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package io.lenses.serde;

// Mimics the Kafka core GroupMetadataKey class.

import io.lenses.serde.utils.Either;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

public class GroupMetadataKey {
  public static Schema Schema =
      SchemaBuilder.builder()
          .record("GroupMetadataKey")
          .fields()
          .requiredInt("version")
          .requiredString("key")
          .endRecord();
  private final short version;
  private final String group;

  public static final short LOWEST_SUPPORTED_VERSION = 2;
  public static final short HIGHEST_SUPPORTED_VERSION = 2;

  public GroupMetadataKey(short version, String group) {
    this.version = version;
    this.group = group;
  }

  public GenericRecord toAvro() {
    GenericRecord record = new org.apache.avro.generic.GenericData.Record(Schema);
    record.put("version", (int) version);
    record.put("key", group);
    return record;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GroupMetadataKey)) return false;
    GroupMetadataKey other = (GroupMetadataKey) obj;
    if (this.group == null) {
      return other.group == null;
    } else {
      return this.group.equals(other.group);
    }
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    hashCode = 31 * hashCode + (group == null ? 0 : group.hashCode());
    return hashCode;
  }

  @Override
  public String toString() {
    return "GroupMetadataKey(" + "group=" + ((group == null) ? "null" : "'" + group + "'") + ")";
  }

  public String group() {
    return this.group;
  }

  public static Either<Throwable, GroupMetadataKey> from(ByteBuffer buffer) {
    short version = buffer.getShort();
    if (version < LOWEST_SUPPORTED_VERSION || version > HIGHEST_SUPPORTED_VERSION) {
      return Either.left(
          new RuntimeException("Invalid/Unsupported group metadata key version:" + version));
    }
    short length = buffer.getShort();
    if (length < 0) {
      return Either.left(new RuntimeException("non-nullable field group was serialized as null"));
    }

    final byte[] groupBytes = new byte[length];
    buffer.get(groupBytes);
    final String group = new String(groupBytes);
    return Either.right(new GroupMetadataKey(version, group));
  }
}
