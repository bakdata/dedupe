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
import lombok.NonNull;

/**
 * Classifies a {@link OnlineCandidate} as duplicate, non-duplicate, or other {@link Classification}s.
 *
 * @apiNote There is no general difference between classification of pairs in online or offline usage.
 * @implSpec A classifier is assumed to work stateless; that is, repeated invocation on the same candidate will yield
 * the same duplicateClass. Derivations need to be documented.
 */
@FunctionalInterface
public interface Classifier<T> {
    /**
     * Classifies the {@link OnlineCandidate} as duplicate, non-duplicate, or other {@link Classification}s.
     *
     * @param candidate the candidate to classify.
     * @return the {@link ClassificationResult} duplicateClass.
     * @throws ClassificationException if one or more exceptions occur during classification.
     */
    @NonNull ClassificationResult classify(@NonNull Candidate<T> candidate);

    /**
     * Classifies the {@link Candidate} as duplicate, non-duplicate, or other {@link Classification}s and stores the
     * {@link ClassificationResult} together with the candidate.
     *
     * @param candidate the candidate to classify.
     * @return the {@link ClassifiedCandidate}.
     */
    default @NonNull ClassifiedCandidate<T> classifyCandidate(final @NonNull Candidate<T> candidate) {
        return new ClassifiedCandidate<>(candidate, this.classify(candidate));
    }
}
