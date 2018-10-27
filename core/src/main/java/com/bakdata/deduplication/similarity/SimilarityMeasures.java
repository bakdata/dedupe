package com.bakdata.deduplication.similarity;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import lombok.*;

import java.time.Duration;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("WeakerAccess")
public class SimilarityMeasures {

    private static final Splitter WHITE_SPACE_SPLITTER = Splitter.on(Pattern.compile("\\s+"));

    public static <T, C extends Collection<T>> SimilarityMeasure<C> jaccard() {
        return (left, right, context) -> {
            @SuppressWarnings("unchecked")
            final Set<T> leftSet = left instanceof Set ? (Set<T>) left : new HashSet<>(left);
            @SuppressWarnings("unchecked")
            final Set<T> rightSet = left instanceof Set ? (Set<T>) right : new HashSet<>(right);
            final long intersectCount = leftSet.stream().filter(rightSet::contains).count();
            return (float) intersectCount / (rightSet.size() + leftSet.size() - intersectCount);
        };
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> max(SimilarityMeasure<? super T>... measures) {
        if (measures.length == 0) {
            throw new IllegalArgumentException();
        }
        return (left, right, context) -> (float) Arrays.stream(measures).mapToDouble(m -> m.getSimilarity(left, right, context)).max().getAsDouble();
    }

    @SafeVarargs
    public static <T> SimilarityMeasure<T> min(SimilarityMeasure<? super T>... measures) {
        if (measures.length == 0) {
            throw new IllegalArgumentException();
        }
        return (left, right, context) -> (float) Arrays.stream(measures).mapToDouble(m -> m.getSimilarity(left, right, context)).min().getAsDouble();
    }

    public static <T> SimilarityMeasure<T> equality() {
        return ((left, right, context) -> left.equals(right) ? 1 : 0);
    }

    public static <T extends Temporal> SimilarityMeasure<T> maxDiff(int diff, TemporalUnit unit) {
        return (left, right, context) ->
                Math.max(0, 1 - (float) Math.abs(left.until(right, unit)) / diff);
    }

    public static <T, R> SimilarityTransformation<T, R> transform(Function<T, R> function) {
        return (t, context) -> function.apply(t);
    }


    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> ngram(int n) {
        return (t, context) -> IntStream.range(0, t.length() - n + 1)
                .mapToObj(i -> t.subSequence(i, i + n))
                .collect(Collectors.toList());
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> bigram() {
        return ngram(2);
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<CharSequence>> trigram() {
        return ngram(3);
    }

    public static <T extends CharSequence> SimilarityTransformation<T, List<String>> tokenize() {
        return (t, context) -> Lists.newArrayList(WHITE_SPACE_SPLITTER.split(t));
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAverage() {
        return weightedAggregation((weightedSims, weights) ->
                (float) (weightedSims.stream().mapToDouble(sim -> sim).sum() / weights.stream().mapToDouble(w -> w).sum()));
    }

    public static <T> WeightedAggregation.WeightedAggregationBuilder<T> weightedAggregation(BiFunction<List<Float>, List<Float>, Float> aggregator) {
        return WeightedAggregation.<T>builder().aggregator(aggregator);
    }

    static int getMaxLen(CharSequence left, CharSequence right) {
        return Math.max(left.length(), right.length());
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
            return aggregator.apply(weightedSims, getWeights());
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

}
