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
    C id;

    List<T> elements;

    public Cluster(C id) {
        this(id, new ArrayList<>());
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
        if(other == this) {
            return this;
        }
        final List<T> concatElements = new ArrayList<>(elements);
        concatElements.addAll(other.getElements());
        return new Cluster<>(idGenerator.apply(concatElements), concatElements);
    }

    @SuppressWarnings("squid:S4276")
    private static final Function<Iterable<?>, Integer> INT_GENERATOR = new Function<>() {
        private final AtomicInteger id = new AtomicInteger();

        @Override
        public Integer apply(Iterable<?> objects) {
            return id.getAndIncrement();
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Function<Iterable<T>, Integer> intGenerator() {
        return (Function) INT_GENERATOR;
    }

    @SuppressWarnings("squid:S4276")
    private static final Function<Iterable<?>, Long> LONG_GENERATOR = new Function<>() {
        private final AtomicLong id = new AtomicLong();

        @Override
        public Long apply(Iterable<?> objects) {
            return id.getAndIncrement();
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Function<Iterable<T>, Long> longGenerator() {
        return (Function) LONG_GENERATOR;
    }
}
