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

import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.experimental.UtilityClass;


/**
 * Utility methods for {@link Cluster}
 */
@UtilityClass
public class Clusters {
    /**
     * Finds the cluster containing a given record and assures that there is exactly one.
     *
     * @throws IllegalArgumentException when there is not exactly one cluster
     */
    public static <C extends Comparable<C>, T, I> @NonNull Cluster<C, T, I> getContainingCluster(
            final @NonNull Iterator<? extends Cluster<C, T, I>> clusterIterator, final @NonNull T record) {
        final Spliterator<Cluster<C, T, I>> spliterator =
                Spliterators.spliteratorUnknownSize(clusterIterator, ORDERED | NONNULL);
        final List<? extends Cluster<C, T, I>> mainClusters = StreamSupport.stream(spliterator, false)
                .filter(c -> c.contains(record))
                .collect(Collectors.toList());
        if (mainClusters.size() != 1) {
            throw new IllegalArgumentException(
                    "Expected exactly one cluster with the new record, but found " + mainClusters);
        }
        return mainClusters.get(0);
    }
}
