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

package com.bakdata.dedupe.similarity;

import lombok.NonNull;
import lombok.Value;

/**
 * Cuts the similarity returned by this similarity, such that all values {@code <threshold} result in a similarity of 0,
 * and all values {@code [threshold, 1]} are left untouched.
 * <p>This method can be used to apply thresholds to parts of compounds similarity measures to avoid false
 * positives resulting from an overall lenient threshold.</p>
 */
@Value
public class CutoffSimiliarityMeasure<T> implements SimilarityMeasure<T> {
    /**
     * The similarity measure that is applied first before applying the threshold.
     */
    @NonNull
    SimilarityMeasure<T> inner;
    /**
     * The threshold that divides the dissimilar and the similar values.
     */
    float threshold;

    /**
     * Cuts the similarity, such that all values {@code <threshold} result in a similarity of 0, and all values {@code
     * [threshold, 1]} are left untouched.
     * <p>This method can be used to apply thresholds to parts of compounds similarity measures to avoid false
     * positives resulting from an overall lenient threshold.</p>
     *
     * @param similarity the similarity to process.
     * @param threshold the threshold that divides the dissimilar and the similar values.
     * @return a similarity measure replacing all similarities {@code <threshold} by 0.
     */
    public static float cutoff(final float similarity, final float threshold) {
        return similarity < threshold ? 0 : similarity;
    }

    @Override
    public float getNonNullSimilarity(@NonNull final T left, @NonNull final T right,
            @NonNull final SimilarityContext context) {
        return cutoff(this.inner.getSimilarity(left, right, context), this.threshold);
    }

    @Override
    public SimilarityMeasure<T> cutoff(final float threshold) {
        if (threshold < this.threshold) {
            return this;
        }
        return new com.bakdata.dedupe.similarity.CutoffSimiliarityMeasure<>(this.inner, threshold);
    }
}
