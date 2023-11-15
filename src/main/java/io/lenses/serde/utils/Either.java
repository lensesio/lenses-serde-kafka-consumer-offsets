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
package io.lenses.serde.utils;

import java.util.function.Consumer;

public class Either<A, B> {
  private final A left;
  private final B right;
  private final boolean isLeft;

  private Either(A left, B right, boolean isLeft) {
    this.left = left;
    this.right = right;
    this.isLeft = isLeft;
  }

  public static <A, B> Either<A, B> left(A left) {
    return new Either<>(left, null, true);
  }

  public static <A, B> Either<A, B> right(B right) {
    return new Either<>(null, right, false);
  }

  public boolean isLeft() {
    return isLeft;
  }

  public boolean isRight() {
    return !isLeft;
  }

  public void ifRightOrElse(Consumer<B> rightConsumer, Consumer<A> leftConsumer) {
    if (isLeft) {
      leftConsumer.accept(left);
    } else {
      rightConsumer.accept(right);
    }
  }

  public A getLeft() {
    return left;
  }

  public B getRight() {
    return right;
  }

  public <C> C fold(Fold<A, B, C> fold) {
    return fold.fold(left, right);
  }

  public interface Fold<A, B, C> {
    C fold(A left, B right);
  }
}
