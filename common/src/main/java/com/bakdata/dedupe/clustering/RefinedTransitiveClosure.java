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

import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Executes {@link TransitiveClosure} and successively {@link RefineCluster}. This algorithm boosts both recall and
 * precision, but is rather compute-heavy.
 *
 * @param <C> the type of the cluster id.
 * @param <T> the type of the record.
 * @param <I> the type of the record id.
 */
@Value
@Builder
public class RefinedTransitiveClosure<C extends Comparable<C>, T, I extends Comparable<? super I>>
        implements Clustering<C, T> {
    /**
     * The configured refineCluster.
     */
    @NonNull
    RefineCluster<C, T> refineCluster;

    /**
     * A backing map for old clusters. Defaults to an in-memory map if null during construction.
     */
    @NonNull
    Map<I, Cluster<C, T>> oldClusterIndex;

    /**
     * The underlying transitive closure implementation.
     */
    @NonNull
    TransitiveClosure<C, T, I> closure;

    /**
     * Extracts the id of the record. Used for {@link #oldClusterIndex}.
     */
    @NonNull
    Function<? super T, ? extends I> idExtractor;

    /**
     * A callback that may veto cluster splits. Defaults to {@link ClusterSplitHandler#ignore()}.
     */
    @NonNull
    ClusterSplitHandler splitHandler;

    @java.beans.ConstructorProperties({"refineCluster", "oldClusterIndex", "closure", "idExtractor", "splitHandler"})
    RefinedTransitiveClosure(final @NonNull RefineCluster<C, T> refineCluster,
            final Map<I, Cluster<C, T>> oldClusterIndex, final TransitiveClosure<C, T, I> closure,
            final @NonNull Function<? super T, ? extends I> idExtractor, final ClusterSplitHandler splitHandler) {
        this.refineCluster = refineCluster;
        this.oldClusterIndex = oldClusterIndex != null ? oldClusterIndex : new HashMap<>();
        this.closure = closure != null ? closure
                : new TransitiveClosure<>(idExtractor, refineCluster.getClusterIdGenerator(), new HashMap<>());
        this.idExtractor = idExtractor;
        this.splitHandler = splitHandler != null ? splitHandler : ClusterSplitHandler.ignore();
    }

    @Override
    public @NonNull Stream<Cluster<C, T>> cluster(final @NonNull Stream<ClassifiedCandidate<T>> classifiedCandidates) {
        final List<ClassifiedCandidate<T>> materializedCandidates = classifiedCandidates.collect(Collectors.toList());
        final @NonNull Stream<Cluster<C, T>> transitiveClosure = this.closure.cluster(materializedCandidates.stream());
        final Stream<Cluster<C, T>> refinedClusters =
                this.refineCluster.refine(transitiveClosure, materializedCandidates.stream());

        final Collection<Cluster<C, T>> changedClusters = new ArrayList<>();
        refinedClusters.forEach(refinedCluster -> {
            for (final T element : refinedCluster.getElements()) {
                final I id = this.idExtractor.apply(element);
                final Cluster<C, T> oldCluster = this.oldClusterIndex.put(id, refinedCluster);
                if (oldCluster == null || !this.getClusterId(oldCluster).equals(this.getClusterId(refinedCluster))) {
                    changedClusters.add(refinedCluster);
                }
            }
        });

        // return the changed clusters but remove multiple occurences of the same cluster
        return changedClusters.stream()
                .collect(Collectors.groupingBy(this::getClusterId))
                .values()
                .stream()
                .map(clusters -> clusters.get(0));
    }

    private I getClusterId(final Cluster<C, ? extends T> cluster) {
        return this.idExtractor.apply(cluster.get(0));
    }

    @Override
    public @NonNull Function<? super Iterable<? extends T>, C> getClusterIdGenerator() {
        return this.closure.getClusterIdGenerator();
    }

}
