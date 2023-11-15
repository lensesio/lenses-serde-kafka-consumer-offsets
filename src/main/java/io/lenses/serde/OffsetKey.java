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

import io.lenses.serde.utils.Either;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/** Mimics the Kafka core OffsetKey class. */
public class OffsetKey {
  public static Schema Schema =
      SchemaBuilder.builder()
          .record("OffsetKey")
          .fields()
          .requiredInt("version")
          .name("key")
          .type(
              SchemaBuilder.record("GroupTopicPartition")
                  .fields()
                  .requiredString("group")
                  .name("topicPartition")
                  .type(
                      SchemaBuilder.record("TopicPartition")
                          .fields()
                          .requiredString("topic")
                          .requiredInt("partition")
                          .endRecord())
                  .noDefault()
                  .endRecord())
          .noDefault()
          .endRecord();
  private final short version;
  private final GroupTopicPartition key;

  public OffsetKey(short version, GroupTopicPartition key) {
    this.version = version;
    this.key = key;
  }

  public GenericRecord toAvro() {
    GenericRecord record = new org.apache.avro.generic.GenericData.Record(Schema);
    record.put("version", version);

    final GroupTopicPartition groupTopicPartition = key();
    final GenericRecord groupTopicPartitionRecord =
        new GenericData.Record(Schema.getField("key").schema());
    groupTopicPartitionRecord.put("group", groupTopicPartition.getGroup());
    final GenericRecord topicPartitionRecord =
        new GenericData.Record(Schema.getField("key").schema().getField("topicPartition").schema());
    topicPartitionRecord.put("topic", groupTopicPartition.getTopicPartition().topic());
    topicPartitionRecord.put("partition", groupTopicPartition.getTopicPartition().partition());
    groupTopicPartitionRecord.put("topicPartition", topicPartitionRecord);
    record.put("key", groupTopicPartitionRecord);
    return record;
  }

  public short version() {
    return version;
  }

  public GroupTopicPartition key() {
    return key;
  }

  /**
   * Deserializes the OffsetKey from a byte array.
   *
   * @param bytes the byte array to deserialize from
   * @return the deserialized OffsetKey wrapped in an Optional.
   */
  public static Either<Throwable, OffsetKey> from(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    short version = buffer.getShort();

    if (version == 0 || version == 1) {
      short length = buffer.getShort();
      if (length < 0) {
        return Either.left(new RuntimeException("non-nullable field group was serialized as null"));
      }

      final byte[] groupBytes = new byte[length];
      buffer.get(groupBytes);
      final String group = new String(groupBytes);

      length = buffer.getShort();
      if (length < 0) {
        return Either.left(new RuntimeException("non-nullable field topic was serialized as null"));
      }
      final byte[] topicBytes = new byte[length];
      buffer.get(topicBytes);
      final String topic = new String(topicBytes);
      final int partition = buffer.getInt();
      return Either.right(
          new OffsetKey(
              version, new GroupTopicPartition(group, new TopicPartition(topic, partition))));
    }
    return Either.left(
        new RuntimeException("Invalid/Unsupported version for offset key: " + version));
  }
}
