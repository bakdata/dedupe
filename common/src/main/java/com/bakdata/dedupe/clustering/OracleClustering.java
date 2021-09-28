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

import com.bakdata.dedupe.classifier.Classification;
import com.bakdata.dedupe.classifier.ClassificationResult;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import com.bakdata.dedupe.classifier.Classifier;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;


/**
 * A clustering that knows the results perfectly as it receives the gold standard during creation.
 * <p>This clustering is used for evaluation if you want to evaluate the effectiveness of {@link
 * com.bakdata.dedupe.candidate_selection.CandidateSelection} or {@link Classifier} without worrying about your {@link
 * Clustering}.</p>
 *
 * @param <C> the type of the cluster id.
 * @param <T> the type of the record.
 * @param <I> the type of the record id.
 */
@Value
public class OracleClustering<C extends Comparable<C>, T, I> implements Clustering<C, T, I> {
    private static final ClassificationResult DUPLICATE =
            ClassificationResult.builder().classification(Classification.DUPLICATE).confidence(1).build();
    private static final ClassificationResult NON_DUPLICATE =
            ClassificationResult.builder().classification(Classification.NON_DUPLICATE).confidence(1).build();
    /**
     * The gold clustering. Every record pair inside a cluster is deemed duplicate and every record pair across clusters
     * is a non-duplicate.
     */
    @NonNull Collection<Cluster<C, T>> goldClusters;
    /**
     * A function to extract the id of a record for efficient, internal data structures.
     */
    @NonNull
    Function<T, I> idExtractor;
    /**
     * Lookup from record id to gold cluster.
     */
    @Getter(value = AccessLevel.PRIVATE, lazy = true)
    Map<I, Cluster<C, T>> idToCluster = this.goldClusters.stream()
            .flatMap(cluster -> cluster.getElements().stream()
                    .map(e -> new AbstractMap.SimpleEntry<>(this.idExtractor.apply(e), cluster)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    @Override
    public @NonNull Stream<Cluster<C, T>> cluster(final @NonNull Stream<ClassifiedCandidate<T>> classifiedCandidates) {
        return classifiedCandidates
                .map(candidate -> this.getIdToCluster()
                        .get(this.idExtractor.apply(candidate.getCandidate().getRecord2())))
                .filter(Objects::nonNull)
                .distinct();
    }

    @Override
    public @NonNull Function<Iterable<? extends I>, C> getClusterIdGenerator() {
        final Map<@NonNull Iterable<? extends I>, C> elementsToClusterId =
                this.goldClusters.stream()
                        .collect(Collectors.toMap(this::getElementIds, Cluster::getId));
        return elementsToClusterId::get;
    }

    private List<I> getElementIds(final Cluster<C, ? extends T> ctiCluster) {
        return ctiCluster.getElements().stream()
                .map(this.idExtractor)
                .collect(Collectors.toList());
    }
}
