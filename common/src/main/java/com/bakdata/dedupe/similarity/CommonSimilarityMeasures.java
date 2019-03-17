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

import com.bakdata.dedupe.matching.BipartiteMatcher;
import com.bakdata.dedupe.matching.WeaklyStableMarriage;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.UtilityClass;
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
    public static <T> SimilarityMeasure<T> equality() {
        return ((left, right, context) -> left.equals(right) ? 1 : 0);
    }

    /**
     * A similarity measure that is 0 if {@code left.equals(right)} or {@code 1} otherwise.
     *
     * @param <T> the type of the record.
     * @return A similarity measure that is 0 if {@code left.equals(right)} or {@code 1} otherwise.
     */
    public static <T> SimilarityMeasure<T> inequality() {
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
    public static <E, C extends Collection<? extends E>> SetSimilarityMeasure<C, E> jaccard() {
        return (left, right, context) -> {
            final long intersectCount = left.stream().filter(right::contains).count();
            return (float) intersectCount / (right.size() + left.size() - intersectCount);
        };
    }

    /**
     * Returns the Levenshtein similarity measure. It is often referred to as edit-distance, but it is actually the most
     * known instance of edit distance.
     * <p>The similarity is calculated by normalizing the number of edits over the maximum input length.</p>
     * <p>Note that Levenshtein distance is really slow, but can be tremulously sped up by using a threshold (e.g.,
     * {@link SimilarityMeasure#cutoff(float)} or {@link SimilarityMeasure#scaleWithThreshold(float)}).</p>
     *
     * @param <T> the type of the record.
     * @return the Levenshtein distance.
     */
    public static <T extends CharSequence> SimilarityMeasure<T> levenshtein() {
        return new Levensthein<>(0);
    }

    /**
     * Jaro-Winkler similarity counts the number of matched and transposed characters with a boost for initial
     * characters.
     *
     * @param <T> the type of the record.
     * @return the Jaro-Winkler similarity.
     */
    public static <T extends CharSequence> SimilarityMeasure<T> jaroWinkler() {
        return new SimilarityScoreMeasure<>(new JaroWinklerDistance());
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
    public static <E, C extends Collection<? extends E>> SimilarityMeasure<C> stableMatching(
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
     * {@link SimilarityMeasure#cutoff(float)} or {@link SimilarityMeasure#scaleWithThreshold(float)}).</p>
     *
     * @param pairMeasure the similarity measure to use to calculate the preferences and the overall similarity.
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return the Monge-Elkan list-based similarity.
     */
    public static <E, C extends Collection<? extends E>> SimilarityMeasure<C> mongeElkan(
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
    public static <E, C extends Collection<? extends E>> SimilarityMeasure<C> matching(BipartiteMatcher<E> matcher,
            final SimilarityMeasure<E> pairMeasure) {
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
    public static <E, C extends Collection<? extends E>> CollectionSimilarityMeasure<C, E> cosine() {
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
     * {@link SimilarityMeasure#cutoff(float)} or {@link SimilarityMeasure#scaleWithThreshold(float)}).</p>
     *
     * @param pairMeasure the similarity measure to use to calculate the preferences and the overall similarity.
     * @param maxPositionDiff the maximum index difference of the left list item and the right list item.
     * @param <E> the element type.
     * @param <C> the collection type.
     * @return the Monge-Elkan list-based similarity.
     */
    public static <E, C extends Collection<? extends E>> SimilarityMeasure<C> mongeElkan(
            final SimilarityMeasure<E> pairMeasure, final int maxPositionDiff) {
        return new MongeElkan<>(pairMeasure, maxPositionDiff, 0);
    }

    /**
     * Swaps the lower and upper bound, such that equal pairs have a similarity of 0 and unequal pairs of 1.
     * <p>In particular, the returned similarity is {@code 1 - this.sim}.</p>
     *
     * @return a negated similarity measure.
     */
    public static <T> SimilarityMeasure<T> negate(final SimilarityMeasure<T> measure) {
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
    public static <E, C extends List<? extends E>> SimilarityMeasure<C> positionWise(
            final SimilarityMeasure<E> pairMeasure) {
        return mongeElkan(pairMeasure, 0);
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> max(final SimilarityMeasure<? super T>... measures) {
        return new AggregatingSimilarityMeasure<T>((Stream<Float> similarities) -> similarities
                .filter(sim -> !SimilarityMeasure.isUnknown(sim))
                .takeWhile(sim -> sim < 1f)
                .max(Float::compareTo)
                .orElse(unknown()), measures);
    }

    public static <T extends Temporal> SimilarityMeasure<T> maxDiff(final int diff, final TemporalUnit unit) {
        return (left, right, context) ->
                Math.max(0, 1 - (float) Math.abs(left.until(right, unit)) / diff);
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> min(final SimilarityMeasure<? super T>... measures) {
        return new AggregatingSimilarityMeasure<T>((Stream<Float> similarities) -> similarities
                .filter(sim -> !SimilarityMeasure.isUnknown(sim))
                .takeWhile(sim -> sim > 0f)
                .min(Float::compareTo)
                .orElse(unknown()), measures);
    }

//    @SafeVarargs
//    public static <T> SimilarityMeasure<T> average(final SimilarityMeasure<? super T>... measures) {
//        return new AggregatingSimilarityMeasure<T>((Stream<Float> similarities) -> similarities
//                .filter(sim -> !SimilarityMeasure.isUnknown(sim))
//                .reduce(Float::sum)
//                .orElse(unknown()), measures);
//    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAggregation(
            final BiFunction<List<Float>, List<Float>, Float> aggregator) {
        return WeightedAggregation.<T>builder().aggregator(aggregator);
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAverage() {
        return weightedAggregation((weightedSims, weights) ->
                (float) (weightedSims.stream().mapToDouble(sim -> sim).sum() / weights.stream().mapToDouble(w -> w)
                        .sum()));
    }

    @RequiredArgsConstructor
    public static class SimilarityScoreMeasure<T extends CharSequence> implements SimilarityMeasure<T> {
        private final SimilarityScore<? extends Number> score;

        @Override
        public float getNonNullSimilarity(final CharSequence left, final CharSequence right,
                final SimilarityContext context) {
            return this.score.apply(left, right).floatValue();
        }
    }

    @Builder
    @Value
    public static class WeightedAggregation<R> implements SimilarityMeasure<R> {
        BiFunction<List<Float>, List<Float>, Float> aggregator;
        @Singular
        List<WeightedSimilarity<R>> weightedSimilarities;
        @Getter(lazy = true)
        List<Float> weights =
                this.weightedSimilarities.stream().map(WeightedSimilarity::getWeight).collect(Collectors.toList());

        @Override
        public float getNonNullSimilarity(final R left, final R right, final SimilarityContext context) {
            final var weightedSims = this.weightedSimilarities.stream()
                    .map(ws -> ws.getMeasure().getSimilarity(left, right, context) * ws.getWeight())
                    .collect(Collectors.toList());
            List<Float> adjustedWeights = null;
            for (int i = 0; i < weightedSims.size(); i++) {
                if (weightedSims.get(i).isNaN()) {
                    if (adjustedWeights == null) {
                        adjustedWeights = new ArrayList<>(this.getWeights());
                    }
                    adjustedWeights.set(i, 0.0f);
                    weightedSims.set(i, 0.0f);
                }
            }
            return this.aggregator.apply(weightedSims, adjustedWeights == null ? this.getWeights() : adjustedWeights);
        }

        @Value
        public static class WeightedSimilarity<T> {
            float weight;
            SimilarityMeasure<T> measure;
        }

        public static class WeightedAggregationBuilder<R> {
            public <T> WeightedAggregationBuilder<R> add(final float weight, final Function<R, ? extends T> extractor,
                    final SimilarityMeasure<? super T> measure) {
                return this.add(weight, measure.of(extractor));
            }

            public WeightedAggregationBuilder<R> add(final float weight, final SimilarityMeasure<R> measure) {
                return this.weightedSimilarity(new WeightedSimilarity<>(weight, measure));
            }
        }
    }

}
