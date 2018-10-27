package com.bakdata.deduplication.clustering;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

@Value
@RequiredArgsConstructor
@EqualsAndHashCode(exclude = "comparator")
public class Cluster<T> {
    List<T> elements;
    Comparator<T> comparator;

    public Cluster(Comparator<T> comparator) {
        this(new ArrayList<>(), comparator);
    }

    public void add(T record) {
        elements.add(-Collections.binarySearch(elements, record, comparator) - 1, record);
    }

    public int size() {
        return elements.size();
    }

    public T get(int index) {
        return elements.get(index);
    }

    public boolean contains(T record) {
        return Collections.binarySearch(elements, record, comparator) >= 0;
    }

    public Cluster<T> merge(Cluster<T> other) {
        if(other == this) {
            return this;
        }
        final List<T> concatElements = new ArrayList<>(elements);
        concatElements.addAll(other.getElements());
        concatElements.sort(comparator);
        return new Cluster<>(concatElements, comparator);
    }
}
