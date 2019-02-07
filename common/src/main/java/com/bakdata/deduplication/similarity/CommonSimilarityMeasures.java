/*
 * The MIT License
 *
 * Copyright (c) 2018 bakdata GmbH
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
 *
 */
package com.bakdata.deduplication.similarity;

import static com.bakdata.deduplication.similarity.SimilarityMeasure.unknown;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.apache.commons.codec.StringEncoder;
import org.apache.commons.codec.language.ColognePhonetic;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.codec.language.bm.BeiderMorseEncoder;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.commons.text.similarity.SimilarityScore;

@SuppressWarnings("WeakerAccess")
@UtilityClass
public class CommonSimilarityMeasures {

    private static final Splitter WHITE_SPACE_SPLITTER = Splitter.on(Pattern.compile("\\s+"));

    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> bigram() {
        return ngram(2);
    }

    public static <T> SimilarityMeasure<T> equality() {
        return ((left, right, context) -> left.equals(right) ? 1 : 0);
    }

    public static <T> SimilarityMeasure<T> inequality() {
        return ((left, right, context) -> left.equals(right) ? 0 : 1);
    }

    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> jaccard() {
        return (left, right, context) -> {
            @SuppressWarnings("unchecked") Set<T> leftSet = left instanceof Set ? (Set<T>) left : new HashSet<>(left);
            @SuppressWarnings("unchecked") Set<T> rightSet = left instanceof Set ? (Set<T>) right : new HashSet<>(right);
            long intersectCount = leftSet.stream().filter(rightSet::contains).count();
            return (float) intersectCount / (rightSet.size() + leftSet.size() - intersectCount);
        };
    }

    public static <T extends CharSequence> SimilarityMeasure<T> levenshtein() {
        return new Levensthein<>(0);
    }

    public static <T extends CharSequence> SimilarityMeasure<T> jaroWinkler() {
        return new SimilarityScoreMeasure<>(new JaroWinklerDistance());
    }

    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> mongeElkan(final SimilarityMeasure<T> pairMeasure) {
        return mongeElkan(pairMeasure, Integer.MAX_VALUE / 2);
    }


    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> cosine() {
        return (left, right, context) -> {
            if (left == null || right == null) {
                return unknown();
            }
            Map<T, Long> leftHistogram = left.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
            Map<T, Long> rightHistogram = right.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
            float dotProduct = 0;
            for (final Map.Entry<T, Long> leftEntry : leftHistogram.entrySet()) {
                Long rightCount = rightHistogram.get(leftEntry.getKey());
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


    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> mongeElkan(final SimilarityMeasure<T> pairMeasure, final int maxPositionDiff) {
        return new MongeElkan<>(pairMeasure, maxPositionDiff, 0);
    }

    public static <T> SimilarityMeasure<T> negate(final SimilarityMeasure<T> measure) {
        return (left, right, context) -> 1 - measure.getSimilarity(left, right, context);
    }

    @SuppressWarnings("unchecked")
    private static <T, C extends Collection<? extends T>> List<T> ensureList(final C leftCollection) {
        return leftCollection instanceof List ? (List<T>) leftCollection : List.copyOf(leftCollection);
    }

    public static <T, C extends List<? extends T>> SimilarityMeasure<C> positionWise(final SimilarityMeasure<T> pairMeasure) {
        return mongeElkan(pairMeasure, 0);
    }

    public static SimilarityTransformation<String, String> colognePhonetic() {
        return codec(new ColognePhonetic());
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

    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> ngram(final int n) {
        return (t, context) -> IntStream.range(0, t.length() - n + 1)
                .mapToObj(i -> t.subSequence(i, i + n))
                .collect(Collectors.toList());
    }

    public static SimilarityTransformation<String, String> soundex() {
        return codec(new Soundex());
    }

    public static SimilarityTransformation<String, String> refinedSoundex(final char[] mapping) {
        return codec(new RefinedSoundex(mapping));
    }

    public static SimilarityTransformation<String, String> beiderMorse() {
        return codec(new BeiderMorseEncoder());
    }

    public static SimilarityTransformation<String, String> codec(final StringEncoder encoder) {
        return (s, context) -> encoder.encode(s);
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<String>> words() {
        return (t, context) -> Lists.newArrayList(WHITE_SPACE_SPLITTER.split(t));
    }

    public static <T, R> SimilarityTransformation<T, R> transform(final Function<T, R> function) {
        return (t, context) -> function.apply(t);
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> trigram() {
        return ngram(3);
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAggregation(final BiFunction<List<Float>, List<Float>, Float> aggregator) {
        return WeightedAggregation.<T>builder().aggregator(aggregator);
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAverage() {
        return weightedAggregation((weightedSims, weights) ->
                (float) (weightedSims.stream().mapToDouble(sim -> sim).sum() / weights.stream().mapToDouble(w -> w).sum()));
    }

    static int getMaxLen(final CharSequence left, final CharSequence right) {
        return Math.max(left.length(), right.length());
    }

    /**
     * Used to translate {@link SimilarityScore} that are actually distance functions to similarity scores
     */
    @RequiredArgsConstructor
    public static class DistanceSimilarityMeasure<T extends CharSequence> implements SimilarityMeasure<T> {
        private final SimilarityScore<? extends Number> score;

        @Override
        public float getSimilarity(final CharSequence left, final CharSequence right, final SimilarityContext context) {
            float distance = this.score.apply(left, right).floatValue();
            if (distance == -1) {
                return 0;
            }
            return 1f - distance / getMaxLen(left, right);
        }
    }

    @RequiredArgsConstructor
    public static class SimilarityScoreMeasure<T extends CharSequence> implements SimilarityMeasure<T> {
        private final SimilarityScore<? extends Number> score;

        @Override
        public float getSimilarity(final CharSequence left, final CharSequence right, final SimilarityContext context) {
            return this.score.apply(left, right).floatValue();
        }
    }

    public static class Levensthein<T extends CharSequence> implements SimilarityMeasure<T> {
        private final float threshold;

        public Levensthein(final float threshold) {
            this.threshold = threshold;
        }

        @Override
        public float getSimilarity(final CharSequence left, final CharSequence right, final SimilarityContext context) {
            final var maxLen = getMaxLen(left, right);
            final var maxDiff = (int) (maxLen * (1 - this.threshold));
            final var measure = new DistanceSimilarityMeasure<T>(new LevenshteinDistance(maxDiff));
            return measure.getSimilarity(left, right, context);
        }

        @Override
        public SimilarityMeasure<T> cutoff(final float threshold) {
            if (threshold < this.threshold) {
                return this;
            }
            return new Levensthein<>(threshold);
        }
    }

    @Builder
    @Value
    public static class WeightedAggregation<R> implements SimilarityMeasure<R> {
        BiFunction<List<Float>, List<Float>, Float> aggregator;
        @Singular
        List<WeightedSimilarity<R>> weightedSimilarities;
        @Getter(lazy = true)
        List<Float> weights = this.weightedSimilarities.stream().map(WeightedSimilarity::getWeight).collect(Collectors.toList());

        @Override
        public float getSimilarity(final R left, final R right, final SimilarityContext context) {
            final var weightedSims = this.weightedSimilarities.stream()
                    .map(ws -> ws.getMeasure().getSimilarity(left, right, context) * ws.getWeight())
                    .collect(Collectors.toList());
            List<Float> adjustedWeights = null;
            for (int i = 0; i < weightedSims.size(); i++) {
                if (weightedSims.get(i).isNaN()) {
                    if (adjustedWeights == null) {
                        adjustedWeights = new ArrayList<>(this.getWeights());
                    }
                    adjustedWeights.set(i, 0f);
                    weightedSims.set(i, 0f);
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
            public <T> WeightedAggregationBuilder<R> add(final float weight, final Function<R, T> extractor, final SimilarityMeasure<T> measure) {
                return this.add(weight, measure.of(extractor));
            }

            public WeightedAggregationBuilder<R> add(final float weight, final SimilarityMeasure<R> measure) {
                return this.weightedSimilarity(new WeightedSimilarity<>(weight, measure));
            }
        }
    }

    @Value
    private static class MongeElkan<C extends Collection<? extends T>, T> implements SimilarityMeasure<C> {
        private final SimilarityMeasure<T> pairMeasure;
        private final int maxPositionDiff;
        private final float cutoff;

        @Override
        public float getSimilarity(final C leftCollection, final C rightCollection, final SimilarityContext context) {
            if (leftCollection.isEmpty() || rightCollection.isEmpty()) {
                return 0;
            }
            final List<T> leftList = ensureList(leftCollection);
            final List<T> rightList = ensureList(rightCollection);
            // when cutoff is .9 and |left| = 3, then on average each element has .1 buffer
            // as soon as the current sum + buffer < index, the cutoff threshold cannot be passed (buffer used up)
            final float cutoffBuffer = (1 - this.cutoff) * leftCollection.size();
            float sum = 0;
            for (int leftIndex = 0; leftIndex < leftCollection.size() && (cutoffBuffer + sum) >= leftIndex; leftIndex++) {
                float max = 0;
                for (int rightIndex = Math.max(0, leftIndex - this.maxPositionDiff),
                     rightMax = Math.min(rightCollection.size(), leftIndex + this.maxPositionDiff); max < 1.0 && rightIndex < rightMax; rightIndex++) {
                    max = Math.max(max, this.pairMeasure
                        .getSimilarity(leftList.get(leftIndex), rightList.get(rightIndex), context));
                }
                sum += max;
            }
            return CutoffSimiliarityMeasure.cutoff(sum / leftCollection.size(), this.cutoff);
        }

        @Override
        public SimilarityMeasure<C> cutoff(final float threshold) {
            return new MongeElkan<>(this.pairMeasure, this.maxPositionDiff, threshold);
        }
    }
}
