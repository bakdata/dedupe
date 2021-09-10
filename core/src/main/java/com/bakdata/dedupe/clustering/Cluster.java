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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;


/**
 * A cluster is a coherent collection of duplicate records.
 * <p>All records inside the cluster are deemed to be pair-wise
 * duplicates. No record outside the cluster is a duplicate with any record inside the cluster.</p>
 * <p>Any dataset can be divided into an exhaustive set of non-overlapping clusters with a {@link
 * com.bakdata.dedupe.classifier.Classifier} and {@link Clustering}. In that sense, the duplicate relation becomes a
 * mathematical partitioning of the dataset.</p>
 * <p>In general, a cluster returned by the {@link Clustering} should always contain at least one element. Temporarily,
 * a cluster may be constructed without elements.</p>
 *
 * @param <C> the type of the cluster id.
 * @param <T> the type of the records.
 * @param <I> the type of the record id.
 */
@Data
@AllArgsConstructor
@Builder
public class Cluster<C extends Comparable<C>, T, I> {
    /**
     * The identifier of the cluster. Typically, it is a short string or an integral type.
     */
    @NonNull
    C id;
    /**
     * The list of elements. While in general there is no order constraints of the elements, a {@link Clustering}
     * implementation may order the elements for faster access.
     */
    @NonNull
    List<T> elements;

    public Cluster(final @NonNull C id) {
        this(id, new ArrayList<>());
    }

    public void add(final @NonNull T record) {
        this.elements.add(record);
    }

    public int size() {
        return this.elements.size();
    }

    public @NonNull T get(final int index) {
        return this.elements.get(index);
    }

    public boolean contains(final @NonNull T record) {
        return this.elements.contains(record);
    }

    /**
     * Merges this cluster with another cluster into one new cluster.
     *
     * @param idGenerator a generator to create the new id.
     * @param idExtractor A function to extract the id of a record.
     * @param other the other cluster.
     * @return the newly created merged cluster or this iff {@code other == this}.
     */
    public @NonNull Cluster<C, T, I> merge(
            final @NonNull Function<? super Iterable<? extends I>, ? extends C> idGenerator,
            final @NonNull Function<? super T, ? extends I> idExtractor,
            final @NonNull Cluster<C, ? extends T, I> other) {
        if (other == this) {
            return this;
        }
        final List<T> concatElements = new ArrayList<>(this.elements);
        concatElements.addAll(other.getElements());
        final List<I> ids = concatElements.stream()
                .map(idExtractor)
                .collect(Collectors.toList());
        return new Cluster<>(idGenerator.apply(ids), concatElements);
    }
}
