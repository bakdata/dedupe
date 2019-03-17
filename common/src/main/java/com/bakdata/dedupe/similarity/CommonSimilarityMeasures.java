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

import com.bakdata.dedupe.matching.WeaklyStableMarriage;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
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
 */
@UtilityClass
public class CommonSimilarityMeasures {

    public static <T> SimilarityMeasure<T> equality() {
        return ((left, right, context) -> left.equals(right) ? 1 : 0);
    }

    public static <T> SimilarityMeasure<T> inequality() {
        return ((left, right, context) -> left.equals(right) ? 0 : 1);
    }

    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> jaccard() {
        return (left, right, context) -> {
            @SuppressWarnings("unchecked") final Set<T> leftSet =
                    left instanceof Set ? (Set<T>) left : new HashSet<>(left);
            @SuppressWarnings("unchecked") final Set<T> rightSet =
                    left instanceof Set ? (Set<T>) right : new HashSet<>(right);
            final long intersectCount = leftSet.stream().filter(rightSet::contains).count();
            return (float) intersectCount / (rightSet.size() + leftSet.size() - intersectCount);
        };
    }

    public static <T extends CharSequence> SimilarityMeasure<T> levenshtein() {
        return new Levensthein<>(0);
    }

    public static <T extends CharSequence> SimilarityMeasure<T> jaroWinkler() {
        return new SimilarityScoreMeasure<>(new JaroWinklerDistance());
    }

    /**
     * Will find the stable matches between the left and the right side with a given pair similarity measure and
     * calculate the average similarity between the stable pairs.
     * <p>A pair (A, B) is stable if neither A or B have another element C, such that A (or B) prefer C over their
     * current partner and that C also prefers the element of his current partner.</p>
     * <p>The average similarity is normalized over the maximum number of elements in left or right, such that
     * superfluous partners degrade the overall similarity.</p>
     *
     * @see <a href="https://en.wikipedia.org/wiki/Stable_marriage_problem">Stable marriage on Wikipedia</a>
     */
    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> stableMatching(
            final SimilarityMeasure<T> pairMeasure) {
        return new MatchingSimilarity<>(new WeaklyStableMarriage<>(), pairMeasure);
    }

    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> mongeElkan(
            final SimilarityMeasure<T> pairMeasure) {
        return mongeElkan(pairMeasure, Integer.MAX_VALUE / 2);
    }

    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> cosine() {
        return (left, right, context) -> {
            final Map<T, Long> leftHistogram =
                    left.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
            final Map<T, Long> rightHistogram =
                    right.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
            float dotProduct = 0;
            for (final Map.Entry<T, Long> leftEntry : leftHistogram.entrySet()) {
                final Long rightCount = rightHistogram.get(leftEntry.getKey());
                if (rightCount != null) {
                    dotProduct += leftEntry.getValue() * rightCount;
                }
            }
            return dotProduct / getLength(leftHistogram) / getLength(rightHistogram);
        };
    }

    private static <T> float getLength(final Map<T, Long> histogram) {
        return (float) Math.sqrt(histogram.values().stream().mapToDouble(count -> count * count).sum());
    }


    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> mongeElkan(
            final SimilarityMeasure<T> pairMeasure, final int maxPositionDiff) {
        return new MongeElkan<>(pairMeasure, maxPositionDiff, 0);
    }

    public static <T> SimilarityMeasure<T> negate(final SimilarityMeasure<? super T> measure) {
        return (left, right, context) -> 1 - measure.getSimilarity(left, right, context);
    }

    public static <T, C extends List<? extends T>> SimilarityMeasure<C> positionWise(
            final SimilarityMeasure<T> pairMeasure) {
        return mongeElkan(pairMeasure, 0);
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> max(final SimilarityMeasure<? super T>... measures) {
        if (measures.length == 0) {
            throw new IllegalArgumentException();
        }
        return (left, right, context) -> {
            if (left == null || right == null) {
                return unknown();
            }
            float max = -1;
            for (int i = 0; max < 1 && i < measures.length; i++) {
                final float similarity = measures[i].getSimilarity(left, right, context);
                if (!Float.isNaN(similarity)) {
                    max = Math.max(similarity, max);
                }
            }
            return max == -1 ? Float.NaN : max;
        };
    }

    public static <T extends Temporal> SimilarityMeasure<T> maxDiff(final int diff, final TemporalUnit unit) {
        return (left, right, context) ->
                Math.max(0, 1 - (float) Math.abs(left.until(right, unit)) / diff);
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> min(final SimilarityMeasure<? super T>... measures) {
        if (measures.length == 0) {
            throw new IllegalArgumentException();
        }
        return (left, right, context) -> {
            if (left == null || right == null) {
                return unknown();
            }
            float min = 2;
            for (int i = 0; min > 0 && i < measures.length; i++) {
                final float similarity = measures[i].getSimilarity(left, right, context);
                if (!Float.isNaN(similarity)) {
                    min = Math.min(similarity, min);
                }
            }
            return min == 2 ? Float.NaN : min;
        };
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAggregation(
            final BiFunction<List<Float>, List<Float>, Float> aggregator) {
        return WeightedAggregation.<T>builder().aggregator(aggregator);
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAverage() {
        return weightedAggregation((weightedSims, weights) ->
                (float) (weightedSims.stream().mapToDouble(sim -> sim).sum() / weights.stream().mapToDouble(w -> w)
                        .sum()));
    }

    static int getMaxLen(final CharSequence left, final CharSequence right) {
        return Math.max(left.length(), right.length());
    }

    @RequiredArgsConstructor
    public static class SimilarityScoreMeasure<T extends CharSequence> implements SimilarityMeasure<T> {
        private final SimilarityScore<? extends Number> score;

        @Override
        public float calculateSimilarity(final CharSequence left, final CharSequence right,
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
        public float calculateSimilarity(final R left, final R right, final SimilarityContext context) {
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
