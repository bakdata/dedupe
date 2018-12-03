/*
 * The MIT License
 *
 * Copyright (c) 2018 bakdata GmbH
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
 *
 */
package com.bakdata.deduplication.clustering;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Value
@RequiredArgsConstructor
@Builder
public class Cluster<C extends Comparable<C>, T> {
    @SuppressWarnings("squid:S4276")
    private static final Function<Iterable<?>, Integer> INT_GENERATOR = new Function<>() {
        private final AtomicInteger id = new AtomicInteger();

        @Override
        public Integer apply(Iterable<?> objects) {
            return id.getAndIncrement();
        }
    };
    @SuppressWarnings("squid:S4276")
    private static final Function<Iterable<?>, Long> LONG_GENERATOR = new Function<>() {
        private final AtomicLong id = new AtomicLong();

        @Override
        public Long apply(Iterable<?> objects) {
            return id.getAndIncrement();
        }
    };
    C id;
    List<T> elements;

    public Cluster(C id) {
        this(id, new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    public static <T> Function<Iterable<T>, Integer> intGenerator() {
        return (Function) INT_GENERATOR;
    }

    @SuppressWarnings("unchecked")
    public static <T> Function<Iterable<T>, Long> longGenerator() {
        return (Function) LONG_GENERATOR;
    }

    public void add(T record) {
        elements.add(record);
    }

    public int size() {
        return elements.size();
    }

    public T get(int index) {
        return elements.get(index);
    }

    public boolean contains(T record) {
        return this.elements.contains(record);
    }

    public Cluster<C, T> merge(Function<Iterable<T>, C> idGenerator, Cluster<C, T> other) {
        if (other == this) {
            return this;
        }
        final List<T> concatElements = new ArrayList<>(elements);
        concatElements.addAll(other.getElements());
        return new Cluster<>(idGenerator.apply(concatElements), concatElements);
    }
}
