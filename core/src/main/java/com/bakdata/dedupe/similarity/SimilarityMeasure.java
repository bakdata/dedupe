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

import java.util.function.DoublePredicate;
import java.util.function.Function;
import lombok.NonNull;

/**
 * A SimilarityMeasure compares two values and calculates a similarity in [-1; 1], where 0 means no similarity and 1
 * equal values in a given context.
 * <p>For example: Consider the edit (Levenshtein) distance.</p>
 * <ul>
 * <li>"aa" and "ab" have a edit distance of 1, which will result in similarity of 0.5.</li>
 * <li>"aa" and "bb" have a edit distance of 2, which will result in similarity of 0.</li>
 * </ul>
 *
 * @param <T> the type to which this similarity measure can be applied
 */
@FunctionalInterface
public interface SimilarityMeasure<T> {
    /**
     * Returns a value indicating that the similarity value is unknown.
     * <p>Currently, unknown is encoded as NaN, which makes computation of compound similarity measures easier.</p>
     * <p>Users that need to directly check for unknown results, should use {@link #isUnknown(double)} to keep the code
     * stable even after that value is changed.</p>
     * <p>Note that unknown should not be used for comparison as {@code unknown() != unknown()}.</p>
     *
     * @see #isUnknown(double)
     */
    @SuppressWarnings("SameReturnValue")
    static double unknown() {
        return Double.NaN;
    }

    /**
     * Checks whether the supplied value is {@link #unknown()}.
     *
     * @param value the value to check.
     * @return true if value is the result of {@code unknown()}.
     */
    static boolean isUnknown(final double value) {
        return Double.isNaN(value);
    }

    /**
     * Returns true if {@code sim(a, b) = sim(b, a)}.
     *
     * @return true if {@code sim(a, b) = sim(b, a)}
     */
    default boolean isSymmetric() {
        return false;
    }

    /**
     * Calculates the similarity of the left and right value.
     * <p>Note that some similarities are non-commutative, so that the order of the parameters matters.</p>
     * <p>In contrast to {@link #getNonNullSimilarity(Object, Object, SimilarityContext)}, this method allows null
     * values for the left or right value.</p>
     *
     * @param left the left element for which the similarity should be calculated.
     * @param right the right element for which the similarity should be calculated.
     * @param context the context of the comparison.
     * @return the similarity [0; 1] or {@link #unknown()} if no comparison can be performed (for example if left or
     * right are null).
     * @implNote the default implementation returns
     * {@link SimilarityContext#getSimilarityForNull(Object, Object, SimilarityContext)} if left or
     * right is null and delegates to {@link #getNonNullSimilarity(Object, Object, SimilarityContext)} otherwise.
     */
    default double getSimilarity(final T left, final T right, final @NonNull SimilarityContext context) {
        if (left == null || right == null) {
            return context.getSimilarityForNull(left, right, context);
        }
        return this.getNonNullSimilarity(left, right, context);
    }

    /**
     * Calculates the similarity of the left and right value.
     * <p>Note that some similarities are non-commutative, so that the order of the parameters matters.</p>
     *
     * @param left the left element for which the similarity should be calculated.
     * @param right the right element for which the similarity should be calculated.
     * @param context the context of the comparison.
     * @return the similarity [0; 1] or {@link #unknown()} if no comparison can be performed (for example if left or
     * right are null).
     */
    double getNonNullSimilarity(@NonNull T left, @NonNull T right, @NonNull SimilarityContext context);

    /**
     * Applies a {@link ValueTransformation} to the left and right value of a similarity comparison before applying this
     * .
     * <p>For example, to compare {@link java.time.LocalDate} with the edit distance, we need to transform it into a
     * formatted string: {@code levenshtein.of(ISO_FORMAT::format)}</p>
     *
     * @param transformer transforms the original input values.
     * @param <O> the output type after the transformation.
     * @return a similarity measure that transforms the input first before applying this similarity measure.
     */
    default <O> @NonNull SimilarityMeasure<O> of(final @NonNull ValueTransformation<O, ? extends T> transformer) {
        return new TransformingSimilarityMeasure<>(transformer, this);
    }

    /**
     * Applies a {@link ValueTransformation} to the left and right value of a similarity comparison before applying this
     * .
     * <p>For example, to compare {@link java.time.LocalDate} with the edit distance, we need to transform it into a
     * formatted string: {@code levenshtein.of(ISO_FORMAT::format)}</p>
     *
     * @param transformer transforms the original input values.
     * @param <O> the output type after the transformation.
     * @return a similarity measure that transforms the input first before applying this similarity measure.
     */
    default <O> @NonNull SimilarityMeasure<O> of(final @NonNull Function<O, ? extends T> transformer) {
        return this.of((outer, context) -> transformer.apply(outer));
    }

    /**
     * Cuts the similarity returned by this similarity, such that all values {@code <threshold} result in a similarity
     * of 0, and all values {@code [threshold, 1]} are left untouched.
     * <p>This method can be used to apply thresholds to parts of compounds similarity measures to avoid false
     * positives resulting from an overall lenient threshold.</p>
     *
     * @param threshold the threshold that divides the dissimilar and the similar values.
     * @return a similarity measure replacing all similarities {@code <threshold} by 0.
     */
    default @NonNull SimilarityMeasure<T> cutoff(final double threshold) {
        return new CutoffSimiliarityMeasure<>(this, threshold);
    }

    /**
     * Scales the similarity returned by this similarity, such that all values {@code ≤minExclusive} result in a
     * similarity of 0, and all values {@code (minExclusive, 1]} are linearly rescaled to {@code (0, 1]}.
     * <p>This method can be used to apply thresholds to parts of compounds similarity measures to avoid false
     * positives resulting from an overall lenient threshold.</p>
     *
     * @param minExclusive the threshold that divides the dissimilar and the similar values.
     * @return a similarity measure rescaling {@code (minExclusive, 1]} to {@code (0, 1]}.
     */
    default @NonNull SimilarityMeasure<T> scaleWithThreshold(final double minExclusive) {
        // uses #cutoff first to allow optimizations for SimilarityMeasure
        final @NonNull SimilarityMeasure<T> similarityMeasure = this.cutoff(minExclusive);
        return (left, right, context) -> {
            final double similarity = similarityMeasure.getSimilarity(left, right, context);
            return similarity > minExclusive ? (similarity - minExclusive) / (1 - minExclusive) : 0;
        };
    }

    /**
     * Binarizes the similarity returned by this similarity, such that all values {@code >0} result in a similarity of
     * 1.
     * <p>This method can be used to treat somewhat dissimilar values equal and is mostly used after applying {@link
     * #cutoff(double)}.</p>
     *
     * @return a similarity measure replacing all similarities {@code >0} with 1.
     */
    default @NonNull SimilarityMeasure<T> binarize() {
        return (left, right, context) -> Math.signum(this.getSimilarity(left, right, context));
    }

    /**
     * Replaces the similarity returned by this similarity, such that all values {@code =0} result in an {@link
     * #unknown()} similarity.
     * <p>This method makes parts of a compound similarity measure optional and is mostly used after applying {@link
     * #cutoff(double)}.</p>
     *
     * @return a similarity measure replacing all similarities {@code =0} with {@link #unknown()}.
     */
    default @NonNull SimilarityMeasure<T> unknownIfZero() {
        return (left, right, context) -> {
            final double similarity = this.getSimilarity(left, right, context);
            return similarity <= 0 ? unknown() : 1.0d;
        };
    }

    /**
     * Replaces the similarity returned by this similarity, such that all values, for which the given predicate
     * evaluates to true, result in an {@link #unknown()} similarity.
     * <p>This method makes parts of a compound similarity measure optional and is mostly used after applying {@link
     * #cutoff(double)}.</p>
     *
     * @return a similarity measure replacing specific similarities with {@link #unknown()}.
     */
    default @NonNull SimilarityMeasure<T> unknownIf(final @NonNull DoublePredicate scorePredicate) {
        return (left, right, context) -> {
            final double similarity = this.getSimilarity(left, right, context);
            return scorePredicate.test(similarity) ? unknown() : similarity;
        };
    }

    /**
     * Swaps the lower and upper bound, such that equal pairs have a similarity of 0 and unequal pairs of 1.
     * <p>In particular, the returned similarity is {@code 1 - this.sim}.</p>
     *
     * @return a negated similarity measure.
     */
    default @NonNull SimilarityMeasure<T> negate() {
        return (left, right, context) -> 1 - this.getSimilarity(left, right, context);
    }
}
