/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.experimental.UtilityClass;


/**
 * Adds some functions that are missing in Java Streams. Although public, this class should be considered an
 * implementation detail. Methods will be removed or changed even within a major.
 * <p>If you need such functions in your code, consider using StreamEx or Jool instead.</p>
 */
@UtilityClass
public class StreamUtil {
    public static <T> @NonNull Stream<T> takeWhileInclusive(final @NonNull Stream<T> stream,
            final @NonNull Predicate<? super T> predicate) {
        final AtomicBoolean test = new AtomicBoolean(true);
        return stream.takeWhile(e -> test.get()).peek(e -> test.set(predicate.test(e)));
    }

    public static @NonNull DoubleStream takeWhileInclusive(final @NonNull DoubleStream stream,
            final @NonNull DoublePredicate predicate) {
        final AtomicBoolean test = new AtomicBoolean(true);
        return stream.takeWhile(e -> test.get()).peek(e -> test.set(predicate.test(e)));
    }

    public static @NonNull IntStream takeWhileInclusive(final @NonNull IntStream stream,
            final @NonNull IntPredicate predicate) {
        final AtomicBoolean test = new AtomicBoolean(true);
        return stream.takeWhile(e -> test.get()).peek(e -> test.set(predicate.test(e)));
    }

    public static @NonNull LongStream takeWhileInclusive(final @NonNull LongStream stream,
            final @NonNull LongPredicate predicate) {
        final AtomicBoolean test = new AtomicBoolean(true);
        return stream.takeWhile(e -> test.get()).peek(e -> test.set(predicate.test(e)));
    }

    public static <T> @NonNull Stream<T> stream(final @NonNull Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
