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
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

@Value
public class OracleClassifier<T> implements Classifier<T> {
    private static final ClassificationResult DUPLICATE =
            ClassificationResult.builder().classification(Classification.DUPLICATE).confidence(1).build();
    private static final ClassificationResult NON_DUPLICATE =
            ClassificationResult.builder().classification(Classification.NON_DUPLICATE).confidence(1).build();

    @NonNull
    Set<Candidate<T>> goldDuplicates;
    @Getter(lazy = true)
    Set<Candidate<T>> symmetricDuplicates = this.calculateSymmetricDuplicates();

    private Set<Candidate<T>> calculateSymmetricDuplicates() {
        return this.getGoldDuplicates().stream()
                .flatMap(
                        duplicate -> Stream.of(duplicate, new OnlineCandidate<>(duplicate.getRecord2(),
                                duplicate.getRecord1())))
                .collect(Collectors.toSet());
    }

    @Override
    public ClassificationResult classify(final Candidate<T> candidate) {
        return this.getSymmetricDuplicates().contains(candidate) ? DUPLICATE : NON_DUPLICATE;
    }
}
