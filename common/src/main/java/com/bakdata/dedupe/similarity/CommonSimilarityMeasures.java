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

import static com.bakdata.dedupe.similarity.SimilarityMeasure.unknown;
import static com.bakdata.util.StreamUtil.takeWhileInclusive;

import com.bakdata.dedupe.matching.BipartiteMatcher;
import com.bakdata.dedupe.matching.WeaklyStableMarriage;
import com.bakdata.dedupe.similarity.WeightedAggregatingSimilarityMeasure.WeightedAggregatingSimilarityMeasureBuilder;
import com.bakdata.dedupe.similarity.WeightedAggregatingSimilarityMeasure.WeightedValue;
import com.google.common.collect.Lists;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.apache.commons.text.similarity.EditDistance;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.commons.text.similarity.SimilarityScore;


/**
 * A utility class that offers factory methods for common similarity measures. Usually, these methods are included
 * through static imports.
 * <p>For new similarity measure, please open an issue or PR on <a href="https://github.com/bakdata/dedupe/">Github</a>
 * }.</p>
 *
 * @see CommonTransformations
 */
@UtilityClass
public class CommonSimilarityMeasures {
    /**
     * A similarity measure that is 1 if {@code left.equals(right)} or {@code 0} otherwise.
     *
     * @param <T> the type of the record.
     * @return A similarity measure that is 1 if {@code left.equals(right)} or {@code 0} otherwise.
     */
    public static <T> @NonNull SimilarityMeasure<T> equality() {
        return ((left, right, context) -> left.equals(right) ? 1 : 0);
    }

    /**
     * A similarity measure that is 0 if {@code left.equals(right)} or {@code 1} otherwise.
     *
     * @param <T> the type of the record.
     * @return A similarity measure that is 0 if {@code left.equals(right)} or {@code 1} otherwise.
     */
    public static <T> @NonNull SimilarityMeasure<T> inequality() {
        return negate(equality());
    }

    /**
     * Treats the input collections as sets (removing duplicate elements) and calculates the size of the intersection
     * over the size of the union.
     *
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return the Jaccard set similarity.
     */
    public static @NonNull <E, C extends Collection<? extends E>> SetSimilarityMeasure<C, E> jaccard() {
        return (left, right, context) -> {
            final long intersectCount = left.stream().filter(right::contains).count();
            return (double) intersectCount / (right.size() + left.size() - intersectCount);
        };
    }

    /**
     * Returns the Levenshtein similarity measure. It is often referred to as edit-distance, but it is actually the most
     * known instance of edit distance.
     * <p>The similarity is calculated by normalizing the number of edits over the maximum input length.</p>
     * <p>Note that Levenshtein distance is really slow, but can be tremulously sped up by using a threshold (e.g.,
     * {@link SimilarityMeasure#cutoff(double)} or {@link SimilarityMeasure#scaleWithThreshold(double)}).</p>
     *
     * @param <T> the type of the record.
     * @return the Levenshtein distance.
     */
    public static @NonNull <T extends CharSequence> SimilarityMeasure<T> levenshtein() {
        return new Levensthein<>(0);
    }

    /**
     * Jaro-Winkler similarity counts the number of matched and transposed characters with a boost for initial
     * characters.
     *
     * @param <T> the type of the record.
     * @return the Jaro-Winkler similarity.
     */
    public static @NonNull <T extends CharSequence> SimilarityMeasure<T> jaroWinkler() {
        return similarityScore(new JaroWinklerDistance());
    }

    /**
     * Wraps a {@link SimilarityScore} of apache commons-text into a {@link SimilarityMeasure}.
     *
     * @param <T> the type of the record.
     * @return the given similarity score wrapped into a SimilarityMeasure.
     */
    public static @NonNull <T extends CharSequence, R extends Number> SimilarityMeasure<T> similarityScore(
            final @NonNull SimilarityScore<R> similarityScore) {
        return (left, right, context) -> similarityScore.apply(left, right).doubleValue();
    }

    /**
     * Used to translate {@link EditDistance} of apache commons-text into a similarity score by using the formula {@code
     * 1 - dist / maxDist} where maxDist is the maximum length of the input strings.
     *
     * @param <T> the type of the record.
     * @return the given edit distance translated into a SimilarityMeasure.
     */
    public static @NonNull <T extends CharSequence, R extends Number> SimilarityMeasure<T> toSimilarity(
            final @NonNull EditDistance<R> editDistance) {
        return (left, right, context) -> {
            final double distance = editDistance.apply(left, right).doubleValue();
            return distance == -1 ? 0 : (1 - distance / Math.max(left.length(), right.length()));
        };
    }

    /**
     * Will find the (weakly) stable matches between the left and the right side with a given pair similarity measure
     * and calculate the average similarity between the stable pairs.
     * <p>A pair (A, B) is (weakly) stable if neither A or B have another element C, such that A (or B) prefer C over
     * their current partner and that C also prefers the element of his current partner.</p>
     * <p>The average similarity is normalized over the maximum number of elements in left or right, such that
     * superfluous partners degrade the overall similarity.</p>
     * <p>Stable matching prefers the left side over the right side.</p>
     *
     * @param pairMeasure the similarity measure to use to calculate the preferences and the overall similarity.
     * @param <E> the element type.
     * @param <C> the collection type.
     * @see <a href="https://en.wikipedia.org/wiki/Stable_marriage_problem">Stable marriage on Wikipedia</a>
     * @return a stable matching similarity.
     */
    public static @NonNull <E, C extends Collection<? extends E>> SimilarityMeasure<C> stableMatching(
            final SimilarityMeasure<E> pairMeasure) {
        return new MatchingSimilarity<>(new WeaklyStableMarriage<>(), pairMeasure);
    }

    /**
     * Monge-Elkan is a simple list-based similarity measure, where elements from the left are matched with elements
     * from the right with the highest similarity within a certain index range.
     * <p>It is simple in the regard that the same element on the right side can be matched multiple times to left
     * elements.</p>
     * <p>For non-repeating matching, consider {@link #matching(BipartiteMatcher, SimilarityMeasure)}.</p>
     * <p>However, it is comparably fast, as only the similarities within the neighborhood are calculated. For a larger
     * neighborhood, the matching approach is usually preferable.</p>
     * <p>Monge-Elkan prefers the left side over the right side.</p>
     * <p>Note that Monge-Elkan distance can be well sped up by using a threshold (e.g.,
     * {@link SimilarityMeasure#cutoff(double)} or {@link SimilarityMeasure#scaleWithThreshold(double)}).</p>
     *
     * @param pairMeasure the similarity measure to use to calculate the preferences and the overall similarity.
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return the Monge-Elkan list-based similarity.
     */
    public static @NonNull <E, C extends Collection<? extends E>> SimilarityMeasure<C> mongeElkan(
            final SimilarityMeasure<E> pairMeasure) {
        return mongeElkan(pairMeasure, Integer.MAX_VALUE / 2);
    }

    /**
     * Uses the given pair {@link SimilarityMeasure} to calculate a preference matrix and uses the {@link
     * BipartiteMatcher} to find the best matching entries. The best matches and their similarity are used to calculate
     * the overall similarity.
     *
     * <p>The average similarity is normalized over the maximum number of elements in left or right, such that
     * superfluous partners degrade the overall similarity.</p>
     *
     * @param pairMeasure the similarity measure to use to calculate the preferences and the overall similarity.
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return a matching similarity measure.
     */
    public static @NonNull <E, C extends Collection<? extends E>> SimilarityMeasure<C> matching(
            final BipartiteMatcher<E> matcher,
            final SimilarityMeasure<? super E> pairMeasure) {
        return new MatchingSimilarity<>(new WeaklyStableMarriage<>(), pairMeasure);
    }

    /**
     * Calculates the cosine similarity measure over two bags of elements.
     * <p>It first calculates the histograms of the two bags, interprets them as count vectors, and computes the cosine
     * similarity.</p>
     * <p>This measure is suited for medium to long texts.</p>
     *
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return the cosine similarity.
     */
    public static @NonNull <E, C extends Collection<? extends E>> CollectionSimilarityMeasure<C, E> cosine() {
        return new CosineSimilarityMeasure<>();
    }

    /**
     * Monge-Elkan is a simple list-based similarity measure, where elements from the left are matched with elements
     * from the right with the highest similarity within a certain index range.
     * <p>It is simple in the regard that the same element on the right side can be matched multiple times to left
     * elements.</p>
     * <p>For non-repeating matching, consider {@link #matching(BipartiteMatcher, SimilarityMeasure)}.</p>
     * <p>However, it is comparably fast, as only the similarities within the neighborhood are calculated. For a larger
     * neighborhood, the matching approach is usually preferable.</p>
     * <p>Monge-Elkan prefers the left side over the right side.</p>
     * <p>Note that Monge-Elkan distance can be well sped up by using a threshold (e.g.,
     * {@link SimilarityMeasure#cutoff(double)} or {@link SimilarityMeasure#scaleWithThreshold(double)}).</p>
     *
     * @param pairMeasure the similarity measure to use to calculate the preferences and the overall similarity.
     * @param maxPositionDiff the maximum index difference of the left list item and the right list item.
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return the Monge-Elkan list-based similarity.
     */
    public static @NonNull <E, C extends Collection<? extends E>> SimilarityMeasure<C> mongeElkan(
            final SimilarityMeasure<E> pairMeasure, final int maxPositionDiff) {
        return new MongeElkan<>(pairMeasure, maxPositionDiff, 0);
    }

    /**
     * Swaps the lower and upper bound, such that equal pairs have a similarity of 0 and unequal pairs of 1.
     * <p>In particular, the returned similarity is {@code 1 - this.sim}.</p>
     * <p>Note that this method offers somewhat better type inference than {@link SimilarityMeasure#negate()}.</p>
     *
     * @param measure the measure to negate.
     * @param <T> the type of the record.
     * @return a negated similarity measure.
     */
    public static <T> @NonNull SimilarityMeasure<T> negate(final SimilarityMeasure<T> measure) {
        return measure.negate();
    }

    /**
     * Performs a simple one to one comparison of elements in two lists based on their index.
     *
     * @param pairMeasure the similarity measure to use to calculate the overall similarity.
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return a position-wise comparing, list-based similarity measure.
     */
    public static @NonNull <E, C extends List<? extends E>> SimilarityMeasure<C> positionWise(
            final SimilarityMeasure<E> pairMeasure) {
        return mongeElkan(pairMeasure, 0);
    }

    /**
     * Returns the largest similarity of the given similarity measures.
     * <p>If all of the similarities are {@link SimilarityMeasure#unknown()}, then the result is also unknown.</p>
     *
     * @param measures the non-empty list of similarity measures.
     * @param <T> the type of the record.
     * @return the largest similarity of the given similarity measures.
     */
    @SafeVarargs
    public static <T> @NonNull SimilarityMeasure<T> max(final SimilarityMeasure<? super T>... measures) {
        return new AggregatingSimilarityMeasure<>(similarities ->
                takeWhileInclusive(similarities, sim -> sim < 1d)
                .max()
                .orElse(unknown()), measures);
    }

    /**
     * Returns the first similarity of the given similarity measures that is not {@link SimilarityMeasure#unknown()}.
     * <p>If all of the similarities are unknown, then the result is also unknown.</p>
     *
     * @param measures the non-empty list of similarity measures.
     * @param <T> the type of the record.
     * @return the first known similarity of the given similarity measures.
     */
    @SafeVarargs
    public static <T> @NonNull SimilarityMeasure<T> first(final SimilarityMeasure<? super T>... measures) {
        return new AggregatingSimilarityMeasure<>(similarities -> similarities
                .filter(sim -> !SimilarityMeasure.isUnknown(sim))
                .findFirst()
                .orElse(unknown()), measures);
    }

    /**
     * Returns the first similarity of the given similarity measures that is not {@link SimilarityMeasure#unknown()}.
     * <p>If all of the similarities are unknown, then the result is also unknown.</p>
     *
     * @param measures the non-empty list of similarity measures.
     * @param <T> the type of the record.
     * @return the first known similarity of the given similarity measures.
     */
    public static <T> @NonNull SimilarityMeasure<T> first(
            final Iterable<? extends SimilarityMeasure<? super T>> measures) {
        return new AggregatingSimilarityMeasure<>(similarities -> similarities
                .filter(sim -> !SimilarityMeasure.isUnknown(sim))
                .findFirst()
                .orElse(unknown()), measures);
    }

    /**
     * Returns the last similarity of the given similarity measures that is not {@link SimilarityMeasure#unknown()}.
     * <p>If all of the similarities are unknown, then the result is also unknown.</p>
     *
     * @param measures the non-empty list of similarity measures.
     * @param <T> the type of the record.
     * @return the last known similarity of the given similarity measures.
     */
    @SafeVarargs
    public static <T> @NonNull SimilarityMeasure<T> last(final SimilarityMeasure<? super T>... measures) {
        final ArrayList<SimilarityMeasure<? super T>> list = Lists.newArrayList(measures);
        Collections.reverse(list);
        return first(list);
    }

    /**
     * Returns the last similarity of the given similarity measures that is not {@link SimilarityMeasure#unknown()}.
     * <p>If all of the similarities are unknown, then the result is also unknown.</p>
     *
     * @param measures the non-empty list of similarity measures.
     * @param <T> the type of the record.
     * @return the last known similarity of the given similarity measures.
     */
    public static <T> @NonNull SimilarityMeasure<T> last(
            final @NonNull Iterable<? extends SimilarityMeasure<? super T>> measures) {
        final ArrayList<SimilarityMeasure<? super T>> list = Lists.newArrayList(measures);
        Collections.reverse(list);
        return first(list);
    }

    /**
     * Calculates the difference between the left and right {@link Temporal} and compares the absolute difference to
     * {@code maxDiff}.
     * <p>A difference of 0 will result in a similarity of 1 and a difference at or over {@code maxDiff} will result in
     * a similarity of 0. Intermediate values will be linearly scaled.</p>
     *
     * @param maxDiff the maximum difference (exclusive) to yield a positive similarity.
     * @param unit the time unit of {@code maxDiff}.
     * @param <T> the type of the record.
     * @return the scaled difference in [0; maxDiff) resulting in (0; 1] or 0 otherwise.
     */
    public static @NonNull <T extends Temporal> SimilarityMeasure<T> scaledDifference(final int maxDiff,
            final TemporalUnit unit) {
        return (left, right, context) ->
                Math.max(0, 1 - (double) Math.abs(left.until(right, unit)) / maxDiff);
    }

    /**
     * Calculates the difference between the left and right number and compares the absolute difference to {@code
     * maxDiff}.
     * <p>A difference of 0 will result in a similarity of 1 and a difference at or over {@code maxDiff} will result in
     * a similarity of 0. Intermediate values will be linearly scaled.</p>
     *
     * @param maxDiff the maximum difference (exclusive) to yield a positive similarity.
     * @param <T> the type of the record.
     * @return the scaled difference in [0; maxDiff) resulting in (0; 1] or 0 otherwise.
     */
    public static @NonNull <T extends Number> SimilarityMeasure<T> scaledDifference(final @NonNull T maxDiff) {
        return (left, right, context) ->
                Math.max(0, 1 - Math.abs(left.doubleValue() - right.doubleValue()) / maxDiff.doubleValue());
    }

    /**
     * Returns the smallest similarity of the given similarity measures.
     * <p>If all of the similarities are {@link SimilarityMeasure#unknown()}, then the result is also unknown.</p>
     *
     * @param measures the non-empty list of similarity measures.
     * @param <T> the type of the record.
     * @return the smallest similarity of the given similarity measures.
     */
    @SafeVarargs
    public static <T> @NonNull SimilarityMeasure<T> min(final SimilarityMeasure<? super T>... measures) {
        return new AggregatingSimilarityMeasure<T>(similarities -> similarities
                .takeWhile(sim -> sim > 0d)
                .min()
                .orElse(unknown()), measures);
    }

    /**
     * Returns the mean similarity of the given similarity measures.
     * <p>If all of the similarities are {@link SimilarityMeasure#unknown()}, then the result is also unknown.</p>
     *
     * @param measures the non-empty list of similarity measures.
     * @param <T> the type of the record.
     * @return the average similarity of the given similarity measures.
     */
    @SafeVarargs
    public static <T> @NonNull SimilarityMeasure<T> mean(final SimilarityMeasure<? super T>... measures) {
        return new AggregatingSimilarityMeasure<T>(similarities -> similarities
                .average()
                .orElse(unknown()), measures);
    }

    /**
     * Starts the creation of a weighted aggregation of multiple {@link SimilarityMeasure}s.
     * <p>{@link SimilarityMeasure#unknown()} values are not passed to the aggregation function.</p>
     *
     * @param aggregator the aggregation function to apply on the similarity values. Premature termination encouraged.
     * @param <T> the type of the record.
     * @return a builder for the weighted aggregation.
     */
    public static <T> @NonNull WeightedAggregatingSimilarityMeasureBuilder<T> weightedAggregation(
            final ToDoubleFunction<Stream<WeightedValue>> aggregator) {
        return WeightedAggregatingSimilarityMeasure.<T>builder().aggregator(aggregator);
    }

    /**
     * Starts the creation of a weighted average of multiple {@link SimilarityMeasure}s.
     * <p>{@link SimilarityMeasure#unknown()} values to not contribute to the average at all.</p>
     *
     * @param <T> the type of the record.
     * @return a builder for the weighted aggregation.
     */
    public static <T> @NonNull WeightedAggregatingSimilarityMeasureBuilder<T> weightedAverage() {
        @Value
        class TotalWeightedValue {
            double weight;
            double weightedValue;

            TotalWeightedValue sum(final TotalWeightedValue other) {
                return new TotalWeightedValue(this.weight + other.weight, this.weightedValue + other.weightedValue);
            }
        }
        return weightedAggregation(weightedValues -> weightedValues
                .map(wv -> new TotalWeightedValue(wv.getWeight(), wv.getWeightedValue()))
                .reduce(TotalWeightedValue::sum)
                .map(total -> total.getWeightedValue() / total.getWeight())
                .orElse(unknown()));
    }

}
