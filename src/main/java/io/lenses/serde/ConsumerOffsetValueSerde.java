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

import com.landoop.lenses.lsql.serde.Deserializer;
import com.landoop.lenses.lsql.serde.Serde;
import com.landoop.lenses.lsql.serde.Serializer;
import io.lenses.serde.utils.Either;
import java.io.IOException;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

public class ConsumerOffsetValueSerde implements Serde {

  public static Schema Schema =
      SchemaBuilder.builder()
          .unionOf()
          .type(OffsetValue.Schema)
          .and()
          .type(GroupMetadataValue.Schema)
          .endUnion();

  public ConsumerOffsetValueSerde() {}

  @Override
  public Serializer serializer(Properties properties) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Deserializer deserializer(Properties properties) {
    return new KeyDeserializer();
  }

  @Override
  public Schema getSchema() {
    return Schema;
  }

  private static class KeyDeserializer implements Deserializer {

    @Override
    public GenericRecord deserialize(byte[] bytes) throws IOException {
      if (bytes == null) {
        return null;
      }
      // Since the key is not passed here, the type cannot be determined upfront. So the code needs
      // to try to deserialize
      // the bytes as both types and return the one that succeeds.
      try {
        final Either<Throwable, OffsetValue> maybeOffsetValue = OffsetValue.from(bytes);
        if (maybeOffsetValue.isLeft()) {
          final Either<Throwable, GroupMetadataValue> maybeGroupValue =
              GroupMetadataValue.from(bytes);
          if (maybeGroupValue.isLeft()) {
            throw new IOException(
                "Failed to deserialize bytes as OffsetValue or GroupMetadataValue",
                maybeGroupValue.getLeft());
          } else {
            return maybeGroupValue.getRight().toAvro();
          }
        } else {
          return maybeOffsetValue.getRight().toAvro();
        }
      } catch (Exception e) {
        final Either<Throwable, GroupMetadataValue> maybeGroupValue =
            GroupMetadataValue.from(bytes);
        if (maybeGroupValue.isLeft()) {
          throw new IOException(
              "Failed to deserialize bytes as OffsetValue or GroupMetadataValue",
              maybeGroupValue.getLeft());
        } else {
          return maybeGroupValue.getRight().toAvro();
        }
      }
    }

    @Override
    public void close() throws IOException {}
  }
}
