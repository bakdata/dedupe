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
package com.bakdata.deduplication.similarity;

import java.util.function.Function;
import java.util.function.Predicate;

@FunctionalInterface
public interface SimilarityMeasure<T> {
    @SuppressWarnings("SameReturnValue")
    static float unknown() {
        return Float.NaN;
    }

    static boolean isUnknown(final float value) {
        return Float.isNaN(value);
    }

    static float scaleWithThreshold(final float similarity, final float min) {
        if (similarity >= min) {
            return (similarity - min) / (1 - min);
        }
        return -(min - similarity) / min;
    }

    float getSimilarity(T left, T right, SimilarityContext context);

    default <O> SimilarityMeasure<O> of(final SimilarityTransformation<O, ? extends T> extractor) {
        return new SimilarityPath<>(extractor, this);
    }

    default <O> SimilarityMeasure<O> of(final Function<O, ? extends T> extractor) {
        return this.of((outer, context) -> extractor.apply(outer));
    }

    default SimilarityMeasure<T> cutoff(final float threshold) {
        return new CutoffSimiliarityMeasure<>(this, threshold);
    }

    default SimilarityMeasure<T> scaleWithThreshold(final float min) {
        return (left, right, context) -> scaleWithThreshold(this.getSimilarity(left, right, context), min);
    }

    default SimilarityMeasure<T> signum() {
        return (left, right, context) -> Math.signum(this.getSimilarity(left, right, context));
    }

    default SimilarityMeasure<T> asBoost() {
        return (left, right, context) -> {
            final float similarity = this.getSimilarity(left, right, context);
            return similarity <= 0 ? unknown() : 1.0f;
        };
    }

    default SimilarityMeasure<T> unknownIf(final Predicate<Float> scorePredicate) {
        return (left, right, context) -> {
            final float similarity = this.getSimilarity(left, right, context);
            return scorePredicate.test(similarity) ? unknown() : similarity;
        };
    }

}
