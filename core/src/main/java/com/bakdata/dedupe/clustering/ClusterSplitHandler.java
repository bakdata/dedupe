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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;


/**
 * A callback that is invoked when an already existing cluster is split up during the {@link Clustering} of (online)
 * deduplication.
 */
@FunctionalInterface
public interface ClusterSplitHandler<C extends Comparable<C>, T, I> {
    /**
     * Do nothing.
     */
    static <C extends Comparable<C>, T, I> @NonNull ClusterSplitHandler<C, T, I> ignore() {
        return (mainCluster, splitParts) -> true;
    }

    /**
     * Invoked when an already existing cluster is split up during the {@link Clustering} of (online) deduplication.
     * <p>The cluster split handle may give a veto by returning false, which the clustering may or may not use.</p>
     *
     * @return false if this handler would like to leave the cluster in-place.
     */
    boolean clusterSplit(@NonNull Cluster<C, T, I> mainCluster, @NonNull List<Cluster<C, T, I>> splitParts);

    /**
     * Checks if the given set of clusters contains more than element and invokes {@link #clusterSplit(Cluster, List)}.
     *
     * @param clusters the clusters resulting from the latest {@link Clustering} invocation.
     * @param newRecord the new record triggering the clustering.
     * @return false if this handler would like to leave the cluster in-place.
     */
    default boolean checkSplit(final @NonNull Collection<? extends Cluster<C, T, I>> clusters,
            final @NonNull T newRecord) {
        if (clusters.size() > 1) {
            final Cluster<C, T, I> mainCluster = Clusters.getContainingCluster(clusters.iterator(), newRecord);
            final List<Cluster<C, T, I>> splitParts = clusters.stream().filter(c -> c != mainCluster).collect(
                    Collectors.toList());
            return this.clusterSplit(mainCluster, splitParts);
        }
        return true;
    }
}
