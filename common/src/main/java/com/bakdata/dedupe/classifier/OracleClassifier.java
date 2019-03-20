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

package com.bakdata.dedupe.classifier;

import com.bakdata.dedupe.candidate_selection.Candidate;
import com.bakdata.dedupe.candidate_selection.online.OnlineCandidate;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;


/**
 * A classifier that knows the results perfectly as it receives the gold standard during creation.
 * <p>This classifier is used for evaluation if you want to evaluate the effectiveness of {@link
 * com.bakdata.dedupe.candidate_selection.CandidateSelection} or {@link com.bakdata.dedupe.clustering.Clustering}
 * without worrying about your {@link Classifier}.</p>
 *
 * @param <T> the type of the record.
 */
@Value
public class OracleClassifier<T> implements Classifier<T> {
    private static final ClassificationResult DUPLICATE =
            ClassificationResult.builder().classification(Classification.DUPLICATE).confidence(1).build();
    private static final ClassificationResult NON_DUPLICATE =
            ClassificationResult.builder().classification(Classification.NON_DUPLICATE).confidence(1).build();

    /**
     * The set of real duplicates. Any pair not within this set is a non-duplicate by definition.
     */
    @NonNull
    Set<Candidate<T>> goldDuplicates;
    /**
     * Adds swapped versions of each duplicate, such that we can perform a fast lookup without considering the element
     * order.
     */
    @Getter(value = AccessLevel.PRIVATE, lazy = true)
    Set<Candidate<T>> symmetricDuplicates = this.calculateSymmetricDuplicates();

    /**
     * For each pair [A, B], also adds [B, A] for fast lookup.
     */
    private Set<Candidate<T>> calculateSymmetricDuplicates() {
        return this.getGoldDuplicates().stream()
                .flatMap(duplicate -> Stream.of(duplicate, new OnlineCandidate<>(duplicate.getRecord2(),
                        duplicate.getRecord1())))
                .collect(Collectors.toSet());
    }

    @Override
    public ClassificationResult classify(final Candidate<T> candidate) {
        // simple lookup
        return this.getSymmetricDuplicates().contains(candidate) ? DUPLICATE : NON_DUPLICATE;
    }
}
