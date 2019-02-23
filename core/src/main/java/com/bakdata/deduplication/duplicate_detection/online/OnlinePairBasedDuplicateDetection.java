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
package com.bakdata.deduplication.duplicate_detection.online;

import com.bakdata.deduplication.candidate_selection.online.OnlineCandidateSelection;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import com.bakdata.deduplication.classifier.Classifier;
import com.bakdata.deduplication.clustering.Cluster;
import com.bakdata.deduplication.clustering.Clustering;
import com.bakdata.deduplication.duplicate_detection.HardPairHandler;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class OnlinePairBasedDuplicateDetection<C extends Comparable<C>, T> implements OnlineDuplicateDetection<C, T> {
    @NonNull
    OnlineCandidateSelection<T> candidateSelection;
    @NonNull
    Classifier<T> classifier;
    @NonNull
    Clustering<C, T> clustering;
    @Builder.Default
    HardPairHandler<T> hardPairHandler = HardPairHandler.ignore();

    @Override
    public List<Cluster<C, T>> detectDuplicates(final T newRecord) {
        final var classified = this.candidateSelection.getCandidates(newRecord)
                .stream()
                .map(candidate -> new ClassifiedCandidate<>(candidate, this.classifier.classify(candidate)))
                .collect(Collectors.toList());

        final var handledPairs = classified.stream()
                .flatMap(cc -> cc.getClassification().getResult()
                        == Classification.ClassificationResult.POSSIBLE_DUPLICATE ?
                        this.hardPairHandler.apply(cc).stream() :
                        Stream.of(cc))
                .collect(Collectors.toList());

        return this.clustering.cluster(handledPairs);
    }
}
