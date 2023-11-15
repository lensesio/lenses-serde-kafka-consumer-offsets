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

import java.util.Objects;

/** Mimics the Kafka core GroupTopicPartition class. */
public class GroupTopicPartition {
  private final String group;
  private final TopicPartition topicPartition;

  public GroupTopicPartition(String group, TopicPartition topicPartition) {
    this.group = group;
    this.topicPartition = topicPartition;
  }

  public String getGroup() {
    return group;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + (this.group != null ? this.group.hashCode() : 0);
    hash = 97 * hash + (this.topicPartition != null ? this.topicPartition.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!GroupTopicPartition.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    final GroupTopicPartition other = (GroupTopicPartition) obj;
    if (!Objects.equals(this.group, other.group)) {
      return false;
    }
    return Objects.equals(this.topicPartition, other.topicPartition);
  }
}
