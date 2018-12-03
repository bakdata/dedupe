/*
 * The MIT License
 *
 * Copyright (c) 2018 bakdata GmbH
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
 *
 */
package com.bakdata.deduplication.deduplication.online;

import com.bakdata.deduplication.candidate_selection.online.OnlineCandidateSelection;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import com.bakdata.deduplication.classifier.Classifier;
import com.bakdata.deduplication.clustering.Cluster;
import com.bakdata.deduplication.clustering.Clustering;
import com.bakdata.deduplication.deduplication.HardFusionHandler;
import com.bakdata.deduplication.deduplication.HardPairHandler;
import com.bakdata.deduplication.fusion.FusedValue;
import com.bakdata.deduplication.fusion.Fusion;
import com.google.common.collect.MoreCollectors;
import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
@Builder
public class OnlinePairBasedDeduplication<T> implements OnlineDeduplication<T> {
    OnlineCandidateSelection<T> candidateSelection;
    Classifier<T> classifier;
    Clustering<?, T> clustering;
    Fusion<T> fusion;
    @Builder.Default
    HardPairHandler<T> hardPairHandler = HardPairHandler.ignore();
    @Builder.Default
    HardFusionHandler<T> hardFusionHandler = HardFusionHandler.dontFuse();

    @Override
    public T deduplicate(T newRecord) {
        var classified = candidateSelection.getCandidates(newRecord)
                .stream()
                .map(candidate -> new ClassifiedCandidate<>(candidate, classifier.classify(candidate)))
                .collect(Collectors.toList());

        var handledPairs = classified.stream()
                .flatMap(cc -> cc.getClassification().getResult() == Classification.ClassificationResult.POSSIBLE_DUPLICATE ?
                        hardPairHandler.apply(cc).stream() :
                        Stream.of(cc))
                .collect(Collectors.toList());

        final List<? extends Cluster<?, T>> clusters = clustering.cluster(handledPairs);
        if (clusters.isEmpty()) {
            return newRecord;
        }

        Cluster<?, T> mainCluster = clusters.stream().filter(c -> c.contains(newRecord)).collect(MoreCollectors.onlyElement());

        return Optional.of(fusion.fuse(mainCluster))
                .flatMap(hardFusionHandler::handlePartiallyFusedValue)
                .map(FusedValue::getValue)
                .orElse(newRecord);
    }
}
