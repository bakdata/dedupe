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
import com.bakdata.dedupe.classifier.Classification;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import com.google.common.collect.Lists;
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
 * An amortized linear transitive closure implementation over the number of pairs.
 *
 * @param <C> the type of the cluster id.
 * @param <T> the type of the record.
 * @param <I> the type of the record id.
 */
@Value
@Builder
public class TransitiveClosure<C extends Comparable<C>, T, I extends Comparable<? super I>>
        implements Clustering<C, T, I> {
    /**
     * Extracts the id of the record. Used for {@link #clusterIndex}.
     */
    @NonNull
    Function<? super T, ? extends I> idExtractor;
    /**
     * A function to generate the id for newly formed clusters.
     */
    @NonNull
    Function<? super Iterable<? extends I>, C> clusterIdGenerator;
    /**
     * A backing map for old clusters. Defaults to an in-memory map if null during construction.
     */
    @NonNull
    @Builder.Default
    Map<I, Cluster<C, T>> clusterIndex = new HashMap<>();

    @Override
    public @NonNull Stream<Cluster<C, T>> cluster(final @NonNull Stream<ClassifiedCandidate<T>> classifiedCandidates) {
        final List<Candidate<T>> duplicates = classifiedCandidates
                .filter(classifiedCandidate -> classifiedCandidate.getClassificationResult().getClassification()
                        == Classification.DUPLICATE)
                .map(ClassifiedCandidate::getCandidate)
                .collect(Collectors.toList());
        return this.clusterDuplicates(duplicates).stream();
    }

    public List<Cluster<C, T>> clusterDuplicates(final Iterable<? extends Candidate<T>> duplicates) {
        final Collection<Cluster<C, T>> changedClusters = new ArrayList<>();

        // apply in-memory transitive closure
        for (final Candidate<T> candidate : duplicates) {
            final Cluster<C, T> leftCluster = this.clusterIndex.get(this.idExtractor.apply(candidate.getRecord1()));
            final Cluster<C, T> rightCluster = this.clusterIndex.get(this.idExtractor.apply(candidate.getRecord2()));
            if (leftCluster == null && rightCluster == null) {
                final List<T> elements = Lists.newArrayList(candidate.getRecord1(), candidate.getRecord2());
                final List<I> ids = this.getElementIds(elements);
                final Cluster<C, T> newCluster = new Cluster<>(this.clusterIdGenerator.apply(ids), elements);
                this.clusterIndex.put(this.idExtractor.apply(candidate.getRecord1()), newCluster);
                this.clusterIndex.put(this.idExtractor.apply(candidate.getRecord2()), newCluster);
                changedClusters.add(newCluster);
            } else if (leftCluster == rightCluster) {
                // nothing to do; already known duplicate
                // still mark it as changed so that downstream processes can work with it
                changedClusters.add(leftCluster);
            } else if (leftCluster == null) {
                rightCluster.add(candidate.getRecord1());
                this.clusterIndex.put(this.idExtractor.apply(candidate.getRecord1()), rightCluster);
                changedClusters.add(rightCluster);
            } else if (rightCluster == null) {
                leftCluster.add(candidate.getRecord2());
                this.clusterIndex.put(this.idExtractor.apply(candidate.getRecord2()), leftCluster);
                changedClusters.add(leftCluster);
            } else { // merge
                final Cluster<C, T> merged = leftCluster.merge(this.clusterIdGenerator, this.idExtractor, rightCluster);
                for (final T person : merged.getElements()) {
                    this.clusterIndex.put(this.idExtractor.apply(person), merged);
                }
                changedClusters.add(merged);
            }
        }

        // return the changed clusters but remove multiple occurrences of the same cluster
        return changedClusters.stream()
                .collect(Collectors.groupingBy(Cluster::getId))
                .values()
                .stream()
                .map(l -> l.get(0))
                .collect(Collectors.toList());
    }

    private List<I> getElementIds(final Collection<? extends T> elements) {
        return elements.stream()
                .map(this.idExtractor)
                .collect(Collectors.toList());
    }

    public void removeCluster(final Cluster<C, ? extends T> cluster) {
        final List<I> recordIds = this.getElementIds(cluster.getElements());
        final Map<C, List<Cluster<C, T>>> referredCluster = recordIds.stream()
                .map(this.clusterIndex::get)
                .collect(Collectors.groupingBy(Cluster::getId));
        if (referredCluster.size() != 1 || !referredCluster.values().iterator().next().get(0).equals(cluster)) {
            throw new IllegalArgumentException("Provided cluster is not known " + cluster);
        }
        this.clusterIndex.keySet().removeAll(recordIds);
    }
}
