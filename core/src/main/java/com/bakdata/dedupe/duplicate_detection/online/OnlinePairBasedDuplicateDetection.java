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
package com.bakdata.dedupe.duplicate_detection.online;

import com.bakdata.dedupe.candidate_selection.Candidate;
import com.bakdata.dedupe.candidate_selection.online.OnlineCandidateSelection;
import com.bakdata.dedupe.classifier.Classification;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import com.bakdata.dedupe.classifier.Classifier;
import com.bakdata.dedupe.clustering.Cluster;
import com.bakdata.dedupe.clustering.Clustering;
import com.bakdata.dedupe.duplicate_detection.PossibleDuplicateHandler;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * A pair-based duplicate detection algorithm, which
 * <ul>
 * <li>Performs {@link com.bakdata.dedupe.candidate_selection.online.OnlineCandidateSelection}</li>
 * <li>Applies a {@link com.bakdata.dedupe.classifier.Classifier} to the found {@link
 * com.bakdata.dedupe.candidate_selection.Candidate}s</li>
 * <li>Transforms the found duplicate pairs with a {@link com.bakdata.dedupe.clustering.Clustering} into {@link
 * com.bakdata.dedupe.clustering.Clusters}s</li>
 * </ul>
 *
 * @param <T> the type of the record
 */
@Value
@Builder
public class OnlinePairBasedDuplicateDetection<C extends Comparable<C>, T> implements OnlineDuplicateDetection<C, T> {
    /**
     * The candidate selection which returns a list of candidates for each new record.
     */
    @NonNull
    OnlineCandidateSelection<T> candidateSelection;
    /**
     * Classifier to label the candidates.
     */
    @NonNull
    Classifier<T> classifier;
    /**
     * Clustering algorithm to form coherent clusters of labeled duplicates.
     */
    @NonNull
    Clustering<C, T> clustering;
    /**
     * A callback for {@link Classification#POSSIBLE_DUPLICATE}s.
     */
    @Builder.Default
    PossibleDuplicateHandler<T> possibleDuplicateHandler = PossibleDuplicateHandler.keep();

    @Override
    public @NonNull Stream<Cluster<C, T>> detectDuplicates(final @NonNull T newRecord) {
        final Stream<Candidate<T>> candidates = this.candidateSelection.selectCandidates(newRecord);
        final var classified = candidates
                .map(candidate -> new ClassifiedCandidate<>(candidate, this.classifier.classify(candidate)))
                .collect(Collectors.toList());

        final var handledPairs = classified.stream()
                .map(cc -> cc.getClassificationResult().getClassification() == Classification.POSSIBLE_DUPLICATE ?
                        this.possibleDuplicateHandler.possibleDuplicateFound(cc) :
                        cc);

        return this.clustering.cluster(handledPairs);
    }
}
