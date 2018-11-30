package com.bakdata.deduplication.similarity;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.*;
import org.apache.commons.codec.StringEncoder;
import org.apache.commons.codec.language.ColognePhonetic;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.codec.language.bm.BeiderMorseEncoder;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.commons.text.similarity.SimilarityScore;

import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.bakdata.deduplication.similarity.SimilarityMeasure.UNKNOWN;

@SuppressWarnings("WeakerAccess")
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
            @SuppressWarnings("unchecked")
            final Set<T> leftSet = left instanceof Set ? (Set<T>) left : new HashSet<>(left);
            @SuppressWarnings("unchecked")
            final Set<T> rightSet = left instanceof Set ? (Set<T>) right : new HashSet<>(right);
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

    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> mongeElkan(SimilarityMeasure<T> pairMeasure) {
        return mongeElkan(pairMeasure, Integer.MAX_VALUE / 2);
    }


    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> cosine() {
        return (left, right, context) -> {
            if(left == null || right == null) {
                return UNKNOWN;
            }
            final Map<T, Long> leftHistogram = left.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
            final Map<T, Long> rightHistogram = right.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
            float dotProduct = 0;
            for (Map.Entry<T, Long> leftEntry : leftHistogram.entrySet()) {
                final Long rightCount = rightHistogram.get(leftEntry.getKey());
                if(rightCount != null) {
                    dotProduct += leftEntry.getValue() * rightCount;
                }
            }
            return dotProduct / getLength(leftHistogram) / getLength(rightHistogram);
        };
    }

    private static <T> float getLength(Map<T, Long> histogram) {
        return (float) Math.sqrt(histogram.values().stream().mapToDouble(count -> count * count).sum());
    }


    public static <T, C extends Collection<? extends T>> SimilarityMeasure<C> mongeElkan(SimilarityMeasure<T> pairMeasure, int maxPositionDiff) {
        return new MongeElkan<>(pairMeasure, maxPositionDiff, 0);
    }

    public static <T> SimilarityMeasure<T> negate(SimilarityMeasure<T> measure) {
        return (left, right, context) -> 1 - measure.getSimilarity(left, right, context);
    }

    private static <T, C extends Collection<? extends T>> List<T> ensureList(C leftCollection) {
        return leftCollection instanceof List ? (List<T>) leftCollection : List.copyOf(leftCollection);
    }

    public static <T, C extends List<? extends T>> SimilarityMeasure<C> positionWise(SimilarityMeasure<T> pairMeasure) {
        return mongeElkan(pairMeasure, 0);
    }

    public static SimilarityTransformation<String, String> colognePhonetic() {
        return codec(new ColognePhonetic());
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> max(SimilarityMeasure<? super T>... measures) {
        if (measures.length == 0) {
            throw new IllegalArgumentException();
        }
        return (left, right, context) -> {
            if(left == null || right == null) {
                return UNKNOWN;
            }
            float max = -1;
            for (int i = 0; max < 1 && i < measures.length; i++) {
                float similarity = measures[i].getSimilarity(left, right, context);
                if (!Float.isNaN(similarity)) {
                    max = Math.max(similarity, max);
                }
            }
            return max == -1 ? Float.NaN : max;
        };
    }

    public static <T extends Temporal> SimilarityMeasure<T> maxDiff(int diff, TemporalUnit unit) {
        return (left, right, context) ->
            Math.max(0, 1 - (float) Math.abs(left.until(right, unit)) / diff);
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> min(SimilarityMeasure<? super T>... measures) {
        if (measures.length == 0) {
            throw new IllegalArgumentException();
        }
        return (left, right, context) ->  {
            if(left == null || right == null) {
                return UNKNOWN;
            }
            float min = 2;
            for (int i = 0; min < 1 && i < measures.length; i++) {
                float similarity = measures[i].getSimilarity(left, right, context);
                if (!Float.isNaN(similarity)) {
                    min = Math.min(similarity, min);
                }
            }
            return min == 2 ? Float.NaN : min;
        };
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> ngram(int n) {
        return (t, context) -> IntStream.range(0, t.length() - n + 1)
            .mapToObj(i -> t.subSequence(i, i + n))
            .collect(Collectors.toList());
    }

    public static SimilarityTransformation<String, String> soundex() {
        return codec(new Soundex());
    }

    public static SimilarityTransformation<String, String> refinedSoundex(char[] mapping) {
        return codec(new RefinedSoundex(mapping));
    }

    public static SimilarityTransformation<String, String> beiderMorse() {
        return codec(new BeiderMorseEncoder());
    }

    public static SimilarityTransformation<String, String> codec(StringEncoder encoder) {
        return (s, context) -> encoder.encode(s);
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<String>> words() {
        return (t, context) -> Lists.newArrayList(WHITE_SPACE_SPLITTER.split(t));
    }

    public static <T, R> SimilarityTransformation<T, R> transform(Function<T, R> function) {
        return (t, context) -> function.apply(t);
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> trigram() {
        return ngram(3);
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAggregation(BiFunction<List<Float>, List<Float>, Float> aggregator) {
        return WeightedAggregation.<T>builder().aggregator(aggregator);
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAverage() {
        return weightedAggregation((weightedSims, weights) ->
            (float) (weightedSims.stream().mapToDouble(sim -> sim).sum() / weights.stream().mapToDouble(w -> w).sum()));
    }

    static int getMaxLen(CharSequence left, CharSequence right) {
        return Math.max(left.length(), right.length());
    }

    /**
     * Used to translate {@link SimilarityScore} that are actually distance functions to similarity scores
     */
    @RequiredArgsConstructor
    public static class DistanceSimilarityMeasure<T extends CharSequence> implements SimilarityMeasure<T> {
        private final SimilarityScore<? extends Number> score;

        @Override
        public float getSimilarity(CharSequence left, CharSequence right, SimilarityContext context) {
            final float score = this.score.apply(left, right).floatValue();
            if(score == -1) {
                return 0;
            }
            return 1f - score / getMaxLen(left, right);
        }
    }

    @RequiredArgsConstructor
    public static class SimilarityScoreMeasure<T extends CharSequence> implements SimilarityMeasure<T> {
        private final SimilarityScore<? extends Number> score;

        @Override
        public float getSimilarity(CharSequence left, CharSequence right, SimilarityContext context) {
            return score.apply(left, right).floatValue();
        }
    }

    public static class Levensthein<T extends CharSequence> implements SimilarityMeasure<T> {
        private final float threshold;

        public Levensthein(float threshold) {
            this.threshold = threshold;
        }

        @Override
        public float getSimilarity(CharSequence left, CharSequence right, SimilarityContext context) {
            var maxLen = getMaxLen(left, right);
            var maxDiff = (int) (maxLen * (1 - threshold));
            var measure = new DistanceSimilarityMeasure<T>(new LevenshteinDistance(maxDiff));
            return measure.getSimilarity(left, right, context);
        }

        @Override
        public SimilarityMeasure<T> cutoff(float threshold) {
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
        List<Float> weights = weightedSimilarities.stream().map(WeightedSimilarity::getWeight).collect(Collectors.toList());

        @Override
        public float getSimilarity(R left, R right, SimilarityContext context) {
            var weightedSims = weightedSimilarities.stream()
                .map(ws -> ws.getMeasure().getSimilarity(left, right, context) * ws.getWeight())
                .collect(Collectors.toList());
            final List<Float> weights = getWeights();
            List<Float> adjustedWeights = null;
            for (int i = 0; i < weightedSims.size(); i++) {
                if(weightedSims.get(i).isNaN()) {
                    if(adjustedWeights == null) {
                        adjustedWeights = new ArrayList<>(weights);
                    }
                    adjustedWeights.set(i, 0f);
                    weightedSims.set(i, 0f);
                }
            }
            return aggregator.apply(weightedSims, adjustedWeights == null ? weights : adjustedWeights);
        }

        @Value
        public static class WeightedSimilarity<T> {
            float weight;
            SimilarityMeasure<T> measure;
        }

        public static class WeightedAggregationBuilder<R> {
            public <T> WeightedAggregationBuilder<R> add(float weight, Function<R, T> extractor, SimilarityMeasure<T> measure) {
                return add(weight, measure.of(extractor));
            }

            public WeightedAggregationBuilder<R> add(float weight, SimilarityMeasure<R> measure) {
                return weightedSimilarity(new WeightedSimilarity<>(weight, measure));
            }
        }
    }

    @Value
    private static class MongeElkan<C extends Collection<? extends T>, T> implements SimilarityMeasure<C> {
        private final SimilarityMeasure<T> pairMeasure;
        private final int maxPositionDiff;
        private final float cutoff;

        @Override
        public float getSimilarity(C leftCollection, C rightCollection, SimilarityContext context) {
            if (leftCollection.isEmpty() || rightCollection.isEmpty()) {
                return 0;
            }
            List<T> leftList = ensureList(leftCollection);
            List<T> rightList = ensureList(rightCollection);
            // when cutoff is .9 and |left| = 3, then on average each element has .1 buffer
            // as soon as the current sum + buffer < index, the cutoff threshold cannot be passed (buffer used up)
            float cutoffBuffer = (1 - cutoff) * leftCollection.size();
            float sum = 0;
            for (int leftIndex = 0; leftIndex < leftCollection.size() && (cutoffBuffer + sum) >= leftIndex; leftIndex++) {
                float max = 0;
                for (int rightIndex = Math.max(0, leftIndex - maxPositionDiff),
                     rightMax = Math.min(rightCollection.size(), leftIndex + maxPositionDiff); max < 1.0 && rightIndex < rightMax; rightIndex++) {
                    max = Math.max(max, pairMeasure.getSimilarity(leftList.get(leftIndex), rightList.get(rightIndex), context));
                }
                sum += max;
            }
            return CutoffSimiliarityMeasure.cutoff(sum / leftCollection.size(), cutoff);
        }

        @Override
        public SimilarityMeasure<C> cutoff(float threshold) {
            return new MongeElkan<>(pairMeasure, maxPositionDiff, threshold);
        }
    }
}
