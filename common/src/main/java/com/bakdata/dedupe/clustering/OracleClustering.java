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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

@Value
public class OracleClustering<C extends Comparable<C>, T, I> implements Clustering<C, T> {
    private static final ClassificationResult DUPLICATE =
            ClassificationResult.builder().classification(Classification.DUPLICATE).confidence(1).build();
    private static final ClassificationResult NON_DUPLICATE =
            ClassificationResult.builder().classification(Classification.NON_DUPLICATE).confidence(1).build();
    Collection<Cluster<C, T>> goldClusters;
    @NonNull
    Function<T, I> idExtractor;
    @Getter(lazy = true)
    Map<I, Cluster<C, T>> idToCluster = this.goldClusters.stream()
            .flatMap(cluster -> cluster.getElements().stream()
                            .map(e -> new AbstractMap.SimpleEntry<>(this.idExtractor.apply(e), cluster)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    @Override
    public @NonNull Iterable<Cluster<C, T>> cluster(
            @NonNull final Iterable<ClassifiedCandidate<T>> classifiedCandidates) {
        return StreamSupport.stream(classifiedCandidates.spliterator(), false)
                .map(candidate -> this.getIdToCluster()
                        .get(this.idExtractor.apply(candidate.getCandidate().getRecord2())))
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public Function<? super Iterable<? extends T>, C> getClusterIdGenerator() {
        return goldClusters.stream().collect(Collectors.toMap((Cluster e) -> e.getElements(), e -> e.getId()))::get;
    }
}
