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

import com.bakdata.dedupe.candidate_selection.Candidate;
import com.bakdata.dedupe.candidate_selection.online.OnlineCandidate;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

/**
 * Wraps another clustering and keeps clusters together, when the wrapped clustering would split it.<br> Example:
 * consider a stable marriage-based clustering where A1-B have been previously matched and subsequently clustered. If a
 * strong A2-B would replace that pair and thus split the cluster, this consistent clustering returns a cluster [A1, A2,
 * B] instead.<br>
 * <p>
 * This clustering is similar to {@link TransitiveClosure} but allows the wrapped clustering to split temporary
 * (=not-returned) clusters. Thus, in the example above, we have the following two situations: - If A1-B and A2-B would
 * be passed in the same invocation of {@link #cluster(Iterable)}}, only cluster [A2, B] would be returned. - If A-B is
 * passed in a first invocation, this invocation returns [A1, B]. The following invocation with A2-B would then return
 * [A1, A2, B].
 * </p>
 * It thus trades off clustering accuracy to increase reliability of subsequent data processing.
 *
 * @implNote This implementation materializes all clusters returned by {@link Clustering}.
 */
@Value
@Builder
public class ConsistentClustering<C extends Comparable<C>, T, I extends Comparable<? super I>>
        implements Clustering<C, T> {
    @NonNull
    Clustering<C, T> clustering;
    Function<T, I> idExtractor;
    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    TransitiveClosure<C, T, I> internalClosure = TransitiveClosure.<C, T, I>builder()
            .idExtractor(this.idExtractor)
            .clusterIdGenerator(this.clustering.getClusterIdGenerator())
            .build();

    @Override
    public @NonNull Iterable<Cluster<C, T>> cluster(
            @NonNull final Iterable<ClassifiedCandidate<T>> classifiedCandidates) {
        final @NonNull List<Cluster<C, T>> clusters =
                Lists.newArrayList(this.clustering.cluster(classifiedCandidates));
        if (clusters.isEmpty()) {
            return clusters;
        }
        // the returned cluster is not affected from this clustering
        if (clusters.size() == 1 && this.noRecordInIndex(clusters)) {
            return clusters;
        }
        final T firstElement = clusters.get(0).get(0);
        final List<Candidate<T>> candidates = clusters.stream()
                .flatMap(cluster -> cluster.getElements().stream()
                        .map(record -> new OnlineCandidate<>(firstElement, record)))
                .collect(Collectors.toList());
        final List<Cluster<C, T>> transitiveClusters = this.getInternalClosure().clusterDuplicates(candidates);
        if (transitiveClusters.size() != 1) {
            throw new IllegalStateException("Expected exactly one transitive cluster");
        }
        if (clusters.size() == 1 && clusters.get(0).equals(transitiveClusters.get(0))) {
            // previously split cluster have been remerged, so we can remove it from our internal closure
            this.getInternalClosure().removeCluster(clusters.get(0));
        }
        return transitiveClusters;
    }

    @Override
    public @NonNull Function<? super Iterable<? extends T>, C> getClusterIdGenerator() {
        return this.clustering.getClusterIdGenerator();
    }

    private boolean noRecordInIndex(final Collection<Cluster<C, T>> clusters) {
        final Map<I, Cluster<C, T>> clusterIndex = this.getInternalClosure().getClusterIndex();
        return clusters.stream().flatMap(cluster -> cluster.getElements().stream())
                .allMatch(record -> clusterIndex.get(this.idExtractor.apply(record)) == null);
    }
}
