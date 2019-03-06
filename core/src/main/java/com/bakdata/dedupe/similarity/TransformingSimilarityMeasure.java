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
 * Applies a {@link SimilarityMeasure} to transformed input values. In particular, the same {@link ValueTransformation}
 * is applied to the left and right input.
 *
 * @param <R> the type of the transformed values.
 * @param <T> the record type.
 */
@Value
public class TransformingSimilarityMeasure<R, T> implements SimilarityMeasure<T> {
    /**
     * The transformation that is applied to both inputs before calculating the similarity.
     */
    private final @NonNull ValueTransformation<T, ? extends R> transformation;
    /**
     * The similarity measure to apply on the transformed values.
     */
    private final @NonNull SimilarityMeasure<R> measure;

    @Override
    public float calculateSimilarity(@NonNull final T left, @NonNull final T right,
            @NonNull final SimilarityContext context) {
        return context.safeExecute(() -> {
            R leftElement = this.transformation.transform(left, context);
            R rightElement = this.transformation.transform(right, context);
            return this.measure.getSimilarity(leftElement, rightElement, context);
        }).orElse(context.getSimilarityMeasureForNull().getSimilarity(left, right, context));
    }

    @Override
    public @NonNull SimilarityMeasure<T> cutoff(final float threshold) {
        return new TransformingSimilarityMeasure<>(this.transformation, this.measure.cutoff(threshold));
    }
}
