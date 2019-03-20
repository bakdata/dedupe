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

package com.bakdata.dedupe.clustering;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.experimental.UtilityClass;

/**
 * A collection of typical cluster id generators.
 */
@UtilityClass
public class ClusterIdGenerators {
    /**
     * Returns an id generator that generates ints starting from 0.
     */
    public static <T> Function<Iterable<? extends T>, Integer> intGenerator() {
        final var nextId = new AtomicInteger();
        return objects -> nextId.getAndIncrement();
    }

    /**
     * Returns an id generator that generates longs starting from 0.
     */
    public static <T> Function<Iterable<? extends T>, Long> longGenerator() {
        final var nextId = new AtomicLong();
        return objects -> nextId.getAndIncrement();
    }

    /**
     * Returns an id generator that generates strings with a given prefix starting from 0.
     */
    public static <T> Function<Iterable<? extends T>, String> stringGenerator(final String prefix) {
        final var nextId = new AtomicLong();
        return objects -> prefix + nextId.getAndIncrement();
    }
}
