package com.bakdata.deduplication.clustering;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Value
@RequiredArgsConstructor
@Builder
public class Cluster<T> {
    Object id;
    List<T> elements;

    public Cluster(Object id) {
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

    public Cluster<T> merge(Function<Iterable<T>, ?> idGenerator, Cluster<T> other) {
        if(other == this) {
            return this;
        }
        final List<T> concatElements = new ArrayList<>(elements);
        concatElements.addAll(other.getElements());
        return new Cluster<>(idGenerator.apply(concatElements), concatElements);
    }

    private static Function<Iterable<?>, Integer> INT_GENERATOR = new Function<>() {
        AtomicInteger id = new AtomicInteger();

        @Override
        public Integer apply(Iterable<?> objects) {
            return id.getAndIncrement();
        }
    };

    public static <T> Function<Iterable<T>, Integer> intGenerator() {
        return (Function) INT_GENERATOR;
    }

    private static Function<Iterable<?>, Long> LONG_GENERATOR = new Function<>() {
        AtomicLong id = new AtomicLong();

        @Override
        public Long apply(Iterable<?> objects) {
            return id.getAndIncrement();
        }
    };


    public static <T> Function<Iterable<T>, Long> longGenerator() {
        return (Function) LONG_GENERATOR;
    }
}
