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
package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class TransitiveClosure<C extends Comparable<C>, T, I extends Comparable<? super I>>
        implements Clustering<C, T> {
    @NonNull
    Function<? super T, ? extends I> idExtractor;
    @NonNull
    Function<Iterable<T>, C> clusterIdGenerator;
    @NonNull
    @Builder.Default
    Map<I, Cluster<C, T>> clusterIndex = new HashMap<>();

    @Override
    public List<Cluster<C, T>> cluster(final List<ClassifiedCandidate<T>> classified) {
        final List<Candidate<T>> duplicates = classified.stream()
                .filter(classifiedCandidate -> classifiedCandidate.getClassification().getResult()
                        == Classification.ClassificationResult.DUPLICATE)
                .map(ClassifiedCandidate::getCandidate)
                .collect(Collectors.toList());
        return this.clusterDuplicates(duplicates);
    }

    public List<Cluster<C, T>> clusterDuplicates(final Iterable<Candidate<T>> duplicates) {
        final Collection<Cluster<C, T>> changedClusters = new ArrayList<>();

        // apply in-memory transitive closure
        for (final Candidate<T> candidate : duplicates) {
            final var leftCluster = this.clusterIndex.get(this.idExtractor.apply(candidate.getNewRecord()));
            final var rightCluster = this.clusterIndex.get(this.idExtractor.apply(candidate.getOldRecord()));
            if (leftCluster == null && rightCluster == null) {
                final List<T> elements = Lists.newArrayList(candidate.getNewRecord(), candidate.getOldRecord());
                final Cluster<C, T> newCluster = new Cluster<>(this.clusterIdGenerator.apply(elements), elements);
                this.clusterIndex.put(this.idExtractor.apply(candidate.getNewRecord()), newCluster);
                this.clusterIndex.put(this.idExtractor.apply(candidate.getOldRecord()), newCluster);
                changedClusters.add(newCluster);
            } else if (leftCluster == rightCluster) {
                // nothing to do; already known duplicate
                // still mark it as changed so that downstream processes can work with it
                changedClusters.add(leftCluster);
            } else if (leftCluster == null) {
                rightCluster.add(candidate.getNewRecord());
                this.clusterIndex.put(this.idExtractor.apply(candidate.getNewRecord()), rightCluster);
                changedClusters.add(rightCluster);
            } else if (rightCluster == null) {
                leftCluster.add(candidate.getOldRecord());
                this.clusterIndex.put(this.idExtractor.apply(candidate.getOldRecord()), leftCluster);
                changedClusters.add(leftCluster);
            } else { // merge
                final Cluster<C, T> merged = leftCluster.merge(this.clusterIdGenerator, rightCluster);
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

    public void removeCluster(final Cluster<C, ? extends T> cluster) {
        final List<I> recordIds = cluster.getElements().stream()
                .map(this.idExtractor)
                .collect(Collectors.toList());
        final Map<C, List<Cluster<C, T>>> referredCluster = recordIds.stream()
                .map(this.clusterIndex::get)
                .collect(Collectors.groupingBy(Cluster::getId));
        if (referredCluster.size() != 1 || !referredCluster.values().iterator().next().get(0).equals(cluster)) {
            throw new IllegalArgumentException("Provided cluster is not known " + cluster);
        }
        this.clusterIndex.keySet().removeAll(recordIds);
    }
}
