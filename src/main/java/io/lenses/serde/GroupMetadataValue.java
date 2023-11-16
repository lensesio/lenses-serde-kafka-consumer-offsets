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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class GroupMetadataValue {

  public static Schema Schema =
      SchemaBuilder.builder()
          .record("GroupValue")
          .fields()
          .requiredInt("generation")
          .requiredString("protocol")
          .requiredString("leader")
          .requiredLong("currentStateTimestamp")
          .name("members")
          .type(
              SchemaBuilder.array()
                  .items(
                      SchemaBuilder.record("MemberMetadata")
                          .fields()
                          .requiredString("memberId")
                          .optionalString("groupInstanceId")
                          .requiredString("clientId")
                          .requiredString("clientHost")
                          .requiredInt("rebalanceTimeout")
                          .requiredInt("sessionTimeout")
                          .optionalBytes("subscription")
                          .optionalBytes("assignment")
                          .endRecord()))
          .noDefault()
          .endRecord();

  private final String protocolType;
  private final int generation;
  private final String protocol;
  private final String leader;
  private final long currentStateTimestamp;
  private final List<MemberMetadata> members;

  public static final short LOWEST_SUPPORTED_VERSION = 0;
  public static final short HIGHEST_SUPPORTED_VERSION = 3;

  public GroupMetadataValue(
      String protocolType,
      int generation,
      String protocol,
      String leader,
      long currentStateTimestamp,
      List<MemberMetadata> members) {
    this.protocolType = protocolType;
    this.generation = generation;
    this.protocol = protocol;
    this.leader = leader;
    this.currentStateTimestamp = currentStateTimestamp;
    this.members = members;
  }

  public GenericRecord toAvro() {
    final GenericRecord record = new GenericData.Record(Schema);
    record.put("generation", generation);
    record.put("protocol", protocol);
    record.put("leader", leader);
    record.put("currentStateTimestamp", currentStateTimestamp);
    final List<GenericRecord> membersAvro = new ArrayList<>();
    for (MemberMetadata memberMetadata : members) {
      final GenericRecord memberRecord =
          new GenericData.Record(Schema.getField("members").schema().getElementType());
      memberRecord.put("memberId", memberMetadata.memberId());
      memberRecord.put("groupInstanceId", memberMetadata.groupInstanceId());
      memberRecord.put("clientId", memberMetadata.clientId());
      memberRecord.put("clientHost", memberMetadata.clientHost());
      memberRecord.put("rebalanceTimeout", memberMetadata.rebalanceTimeout());
      memberRecord.put("sessionTimeout", memberMetadata.sessionTimeout());
      memberRecord.put("subscription", memberMetadata.subscription());
      memberRecord.put("assignment", memberMetadata.assignment());
      membersAvro.add(memberRecord);
    }
    record.put("members", membersAvro);
    return record;
  }

  public static Either<Throwable, GroupMetadataValue> from(byte[] bytes) {
    return from(ByteBuffer.wrap(bytes));
  }

  public static Either<Throwable, GroupMetadataValue> from(ByteBuffer buffer) {
    final short version = buffer.getShort();
    if (version < LOWEST_SUPPORTED_VERSION || version > HIGHEST_SUPPORTED_VERSION) {
      return Either.left(
          new RuntimeException("Invalid/Unsupported group metadata value version:" + version));
    }
    final Either<Throwable, String> maybeProtocolType = readString(buffer, "protocolType");
    if (maybeProtocolType.isLeft()) {
      return Either.left(maybeProtocolType.getLeft());
    }
    final String protocolType = maybeProtocolType.getRight();

    final int generation = buffer.getInt();

    final Either<Throwable, String> maybeProtocol = readStringAllowNull(buffer, "protocol");
    if (maybeProtocol.isLeft()) {
      return Either.left(maybeProtocol.getLeft());
    }
    final String protocol = maybeProtocol.getRight();

    final Either<Throwable, String> maybeLeader = readStringAllowNull(buffer, "leader");
    if (maybeLeader.isLeft()) {
      return Either.left(maybeLeader.getLeft());
    }
    final String leader = maybeLeader.getRight();

    final long currentStateTimestamp = version >= 2 ? buffer.getLong() : -1L;

    final int arrayLength = buffer.getInt();
    if (arrayLength < 0) {
      return Either.left(new RuntimeException("non-nullable field members was serialized as null"));
    }
    if (arrayLength > buffer.remaining()) {
      return Either.left(
          new RuntimeException(
              "Tried to allocate a collection of size "
                  + arrayLength
                  + ", but there are only "
                  + buffer.remaining()
                  + " bytes remaining."));
    }
    final ArrayList<MemberMetadata> members = new ArrayList<>(arrayLength);
    for (int i = 0; i < arrayLength; i++) {
      final Either<Throwable, MemberMetadata> maybeMemberMetadata =
          MemberMetadata.parse(buffer, version);
      if (maybeMemberMetadata.isLeft()) {
        return Either.left(maybeMemberMetadata.getLeft());
      }
      members.add(maybeMemberMetadata.getRight());
    }
    return Either.right(
        new GroupMetadataValue(
            protocolType, generation, protocol, leader, currentStateTimestamp, members));
  }

  private static Either<Throwable, String> readString(ByteBuffer buffer, String field) {
    final int length = buffer.getShort();
    if (length < 0) {
      return Either.left(
          new RuntimeException("non-nullable field " + field + " was serialized as null"));
    } else {
      final byte[] bytes = new byte[length];
      buffer.get(bytes);
      return Either.right(new String(bytes, StandardCharsets.UTF_8));
    }
  }

  private static Either<Throwable, String> readStringAllowNull(ByteBuffer buffer, String field) {
    final int length = buffer.getShort();
    if (length < 0) {
      return Either.right("");
    } else {
      final byte[] bytes = new byte[length];
      buffer.get(bytes);
      return Either.right(new String(bytes, StandardCharsets.UTF_8));
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GroupMetadataValue)) return false;
    GroupMetadataValue other = (GroupMetadataValue) obj;
    if (this.protocolType == null) {
      if (other.protocolType != null) return false;
    } else {
      if (!this.protocolType.equals(other.protocolType)) return false;
    }
    if (generation != other.generation) return false;
    if (this.protocol == null) {
      if (other.protocol != null) return false;
    } else {
      if (!this.protocol.equals(other.protocol)) return false;
    }
    if (this.leader == null) {
      if (other.leader != null) return false;
    } else {
      if (!this.leader.equals(other.leader)) return false;
    }
    if (currentStateTimestamp != other.currentStateTimestamp) return false;
    if (this.members == null) {
      if (other.members != null) return false;
    } else {
      if (!this.members.equals(other.members)) return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    hashCode = 31 * hashCode + (protocolType == null ? 0 : protocolType.hashCode());
    hashCode = 31 * hashCode + generation;
    hashCode = 31 * hashCode + (protocol == null ? 0 : protocol.hashCode());
    hashCode = 31 * hashCode + (leader == null ? 0 : leader.hashCode());
    hashCode = 31 * hashCode + ((int) (currentStateTimestamp >> 32) ^ (int) currentStateTimestamp);
    hashCode = 31 * hashCode + (members == null ? 0 : members.hashCode());
    return hashCode;
  }

  @Override
  public String toString() {
    return "GroupMetadataValue("
        + "protocolType="
        + ((protocolType == null) ? "null" : "'" + protocolType + "'")
        + ", generation="
        + generation
        + ", protocol="
        + ((protocol == null) ? "null" : "'" + protocol + "'")
        + ", leader="
        + ((leader == null) ? "null" : "'" + leader + "'")
        + ", currentStateTimestamp="
        + currentStateTimestamp
        + ", members="
        + deepToString(members.iterator())
        + ")";
  }

  public String protocolType() {
    return this.protocolType;
  }

  public int generation() {
    return this.generation;
  }

  public String protocol() {
    return this.protocol;
  }

  public String leader() {
    return this.leader;
  }

  public long currentStateTimestamp() {
    return this.currentStateTimestamp;
  }

  public List<MemberMetadata> members() {
    return this.members;
  }

  public static String deepToString(Iterator<?> iter) {
    StringBuilder bld = new StringBuilder("[");
    String prefix = "";
    while (iter.hasNext()) {
      Object object = iter.next();
      bld.append(prefix);
      bld.append(object.toString());
      prefix = ", ";
    }
    bld.append("]");
    return bld.toString();
  }

  public static class MemberMetadata {
    private final String memberId;
    private final String groupInstanceId;
    private final String clientId;
    private final String clientHost;
    private final int rebalanceTimeout;
    private final int sessionTimeout;
    private final byte[] subscription;
    private final byte[] assignment;

    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 3;

    public MemberMetadata(
        String memberId,
        String groupInstanceId,
        String clientId,
        String clientHost,
        int rebalanceTimeout,
        int sessionTimeout,
        byte[] subscription,
        byte[] assignment) {
      this.memberId = memberId;
      this.groupInstanceId = groupInstanceId;
      this.clientId = clientId;
      this.clientHost = clientHost;
      this.rebalanceTimeout = rebalanceTimeout;
      this.sessionTimeout = sessionTimeout;
      this.subscription = subscription;
      this.assignment = assignment;
    }

    public static Either<Throwable, MemberMetadata> parse(ByteBuffer buffer, short version) {
      final Either<Throwable, String> maybeMemberId = readString(buffer, "memberId");
      if (maybeMemberId.isLeft()) {
        return Either.left(maybeMemberId.getLeft());
      }
      final String memberId = maybeMemberId.getRight();

      final Either<Throwable, String> maybeGroupInstanceId =
          version >= 3 ? readStringAllowNull(buffer, "groupInstanceId") : Either.right(null);
      if (maybeGroupInstanceId.isLeft()) {
        return Either.left(maybeGroupInstanceId.getLeft());
      }
      final String groupInstanceId = maybeGroupInstanceId.getRight();

      final Either<Throwable, String> maybeClientId = readString(buffer, "clientId");
      if (maybeClientId.isLeft()) {
        return Either.left(maybeClientId.getLeft());
      }
      final String clientId = maybeClientId.getRight();

      final Either<Throwable, String> maybeClientHost = readString(buffer, "clientHost");
      if (maybeClientHost.isLeft()) {
        return Either.left(maybeClientHost.getLeft());
      }
      final String clientHost = maybeClientHost.getRight();

      final int rebalanceTimeout = version >= 1 ? buffer.getInt() : 0;
      final int sessionTimeout = buffer.getInt();

      final Either<Throwable, byte[]> maybeSubscription = readArray(buffer, "subscription");
      if (maybeSubscription.isLeft()) {
        return Either.left(maybeSubscription.getLeft());
      }
      final byte[] subscription = maybeSubscription.getRight();
      final Either<Throwable, byte[]> maybeAssignment = readArray(buffer, "assignment");
      if (maybeAssignment.isLeft()) {
        return Either.left(maybeAssignment.getLeft());
      }
      final byte[] assignment = maybeAssignment.getRight();
      return Either.right(
          new MemberMetadata(
              memberId,
              groupInstanceId,
              clientId,
              clientHost,
              rebalanceTimeout,
              sessionTimeout,
              subscription,
              assignment));
    }

    private static Either<Throwable, byte[]> readArray(ByteBuffer buffer, String subscription) {
      final int length = buffer.getInt();
      if (length < 0) {
        return Either.left(
            new RuntimeException("non-nullable field " + subscription + " was serialized as null"));
      } else {
        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return Either.right(bytes);
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MemberMetadata)) return false;
      MemberMetadata other = (MemberMetadata) obj;
      if (this.memberId == null) {
        if (other.memberId != null) return false;
      } else {
        if (!this.memberId.equals(other.memberId)) return false;
      }
      if (this.groupInstanceId == null) {
        if (other.groupInstanceId != null) return false;
      } else {
        if (!this.groupInstanceId.equals(other.groupInstanceId)) return false;
      }
      if (this.clientId == null) {
        if (other.clientId != null) return false;
      } else {
        if (!this.clientId.equals(other.clientId)) return false;
      }
      if (this.clientHost == null) {
        if (other.clientHost != null) return false;
      } else {
        if (!this.clientHost.equals(other.clientHost)) return false;
      }
      if (rebalanceTimeout != other.rebalanceTimeout) return false;
      if (sessionTimeout != other.sessionTimeout) return false;
      if (!Arrays.equals(this.subscription, other.subscription)) return false;
      return Arrays.equals(this.assignment, other.assignment);
    }

    @Override
    public int hashCode() {
      int hashCode = 0;
      hashCode = 31 * hashCode + (memberId == null ? 0 : memberId.hashCode());
      hashCode = 31 * hashCode + (groupInstanceId == null ? 0 : groupInstanceId.hashCode());
      hashCode = 31 * hashCode + (clientId == null ? 0 : clientId.hashCode());
      hashCode = 31 * hashCode + (clientHost == null ? 0 : clientHost.hashCode());
      hashCode = 31 * hashCode + rebalanceTimeout;
      hashCode = 31 * hashCode + sessionTimeout;
      hashCode = 31 * hashCode + Arrays.hashCode(subscription);
      hashCode = 31 * hashCode + Arrays.hashCode(assignment);
      return hashCode;
    }

    @Override
    public String toString() {
      return "MemberMetadata("
          + "memberId="
          + ((memberId == null) ? "null" : "'" + memberId + "'")
          + ", groupInstanceId="
          + ((groupInstanceId == null) ? "null" : "'" + groupInstanceId.toString() + "'")
          + ", clientId="
          + ((clientId == null) ? "null" : "'" + clientId + "'")
          + ", clientHost="
          + ((clientHost == null) ? "null" : "'" + clientHost + "'")
          + ", rebalanceTimeout="
          + rebalanceTimeout
          + ", sessionTimeout="
          + sessionTimeout
          + ", subscription="
          + Arrays.toString(subscription)
          + ", assignment="
          + Arrays.toString(assignment)
          + ")";
    }

    public String memberId() {
      return this.memberId;
    }

    public String groupInstanceId() {
      return this.groupInstanceId;
    }

    public String clientId() {
      return this.clientId;
    }

    public String clientHost() {
      return this.clientHost;
    }

    public int rebalanceTimeout() {
      return this.rebalanceTimeout;
    }

    public int sessionTimeout() {
      return this.sessionTimeout;
    }

    public byte[] subscription() {
      return this.subscription;
    }

    public byte[] assignment() {
      return this.assignment;
    }
  }
}
