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

/** Mimics the Kafka core OffsetAndMetadata class. */
public class OffsetValue {

  public static Schema Schema =
      SchemaBuilder.builder()
          .record("OffsetValue")
          .fields()
          .requiredLong("offset")
          .optionalInt("leaderEpoch")
          .optionalString("metadata")
          .optionalLong("commitTimestamp")
          .optionalLong("expireTimestamp")
          .endRecord();

  private final long offset;

  private final int leaderEpoch;

  private final String metadata;

  private final long commitTimestamp;

  private final long expireTimestamp;

  public OffsetValue(
      long offset,
      Integer leaderEpoch,
      String metadata,
      long commitTimestamp,
      Long expireTimestamp) {
    this.offset = offset;
    this.leaderEpoch = leaderEpoch;
    this.metadata = metadata;
    this.commitTimestamp = commitTimestamp;
    this.expireTimestamp = expireTimestamp;
  }

  public GenericRecord toAvro() {
    GenericRecord record = new GenericData.Record(Schema);
    record.put("offset", offset);
    record.put("leaderEpoch", leaderEpoch);
    record.put("metadata", metadata);
    record.put("commitTimestamp", commitTimestamp);
    record.put("expireTimestamp", expireTimestamp);
    return record;
  }

  public long offset() {
    return offset;
  }

  public int leaderEpoch() {
    return leaderEpoch;
  }

  public String metadata() {
    return metadata;
  }

  public long commitTimestamp() {
    return commitTimestamp;
  }

  public long expireTimestamp() {
    return expireTimestamp;
  }

  public static final short LOWEST_SUPPORTED_VERSION = 0;
  public static final short HIGHEST_SUPPORTED_VERSION = 3;

  public static Either<Throwable, OffsetValue> from(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return from(buffer);
  }
  /**
   * Deserializes the OffsetAndMetadata to a byte array.
   *
   * @param buffer the buffer to deserialize from
   * @return the deserialized OffsetAndMetadata
   */
  public static Either<Throwable, OffsetValue> from(ByteBuffer buffer) {
    if (buffer == null) {
      return Either.right(null);
    }
    short version = buffer.getShort();
    if (version >= LOWEST_SUPPORTED_VERSION && version <= HIGHEST_SUPPORTED_VERSION) {
      final long offset = buffer.getLong();
      int leaderEpoch = -1;
      if (version >= 3) {
        leaderEpoch = buffer.getInt();
      }
      final short metadataLength = buffer.getShort();
      byte[] metadata = new byte[metadataLength];
      buffer.get(metadata);
      long commitTimestamp = buffer.getLong();
      long expireTimestamp = -1;
      if (version == 1) {
        expireTimestamp = buffer.getLong();
      }
      return Either.right(
          new OffsetValue(
              offset, leaderEpoch, new String(metadata), commitTimestamp, expireTimestamp));
    } else {
      return Either.left(
          new IllegalArgumentException("Unknown offset message version: " + version));
    }
  }
}
