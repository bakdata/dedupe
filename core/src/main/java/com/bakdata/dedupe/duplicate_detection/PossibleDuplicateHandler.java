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
package com.bakdata.dedupe.duplicate_detection;

import com.bakdata.dedupe.classifier.Classification;
import com.bakdata.dedupe.classifier.ClassificationResult;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import lombok.NonNull;


/**
 * A callback that is invoked when a {@link com.bakdata.dedupe.classifier.Classification#POSSIBLE_DUPLICATE} has been
 * founds during {@link DuplicateDetection}.
 *
 * @param <T> the type of the record
 */
@FunctionalInterface
public interface PossibleDuplicateHandler<T> {
    /**
     * Ignores possible duplicates and returns them as is, which will most likely result in dropping them.
     *
     * @param <T> the type of the record
     * @return a handler that ignores possible duplicates.
     */
    static <T> @NonNull PossibleDuplicateHandler<T> keep() {
        return classifiedCandidate -> classifiedCandidate;
    }

    /**
     * Treats possible duplicates as full duplicates.
     *
     * @param <T> the type of the record
     * @return a handler that promotes possible duplicates.
     */
    static <T> @NonNull PossibleDuplicateHandler<T> promoteToDuplicate() {
        final ClassificationResult noDuplicate = ClassificationResult.builder()
                .classification(Classification.DUPLICATE)
                .confidence(0)
                .explanation("Promoted possible duplicate")
                .build();
        return classifiedCandidate -> classifiedCandidate.toBuilder().classificationResult(noDuplicate).build();
    }

    /**
     * Treats possible duplicates as non-duplicates.
     *
     * @param <T> the type of the record
     * @return a handler that demotes possible duplicates.
     */
    static <T> @NonNull PossibleDuplicateHandler<T> demoteToNonDuplicate() {
        final ClassificationResult noDuplicate = ClassificationResult.builder()
                .classification(Classification.NON_DUPLICATE)
                .confidence(0)
                .explanation("Ignored possible duplicate")
                .build();
        return classifiedCandidate -> classifiedCandidate.toBuilder().classificationResult(noDuplicate).build();
    }

    /**
     * Treats possible duplicates as unknown classification.
     *
     * @param <T> the type of the record
     * @return a handler that replaces possible duplicates by unknown values.
     */
    static <T> @NonNull PossibleDuplicateHandler<T> unknown() {
        final ClassificationResult unknown = ClassificationResult.builder()
                .classification(Classification.UNKNOWN)
                .confidence(0)
                .explanation("Ignored possible duplicate")
                .build();
        return classifiedCandidate -> classifiedCandidate.toBuilder().classificationResult(unknown).build();
    }

    /**
     * Invoked when a {@link com.bakdata.dedupe.classifier.Classification#POSSIBLE_DUPLICATE} has been founds during
     * {@link DuplicateDetection}.
     *
     * @param classifiedCandidate the possible duplicate with classification.
     * @return a possibly new classification.
     */
    @NonNull ClassifiedCandidate<T> possibleDuplicateFound(@NonNull ClassifiedCandidate<T> classifiedCandidate);
}
