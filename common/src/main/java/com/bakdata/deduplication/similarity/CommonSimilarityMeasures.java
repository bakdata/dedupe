package com.bakdata.deduplication.similarity;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
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

import static com.bakdata.deduplication.similarity.SimilarityMeasures.getMaxLen;

@SuppressWarnings("WeakerAccess")
public class CommonSimilarityMeasures {

    private static final Splitter WHITE_SPACE_SPLITTER = Splitter.on(Pattern.compile("\\s+"));

    public static <T extends CharSequence> SimilarityMeasure<T> levenshtein() {
        return new Levensthein<>(0);
    }

    public static <T extends CharSequence> SimilarityMeasure<T> jaroWinkler() {
        return new SimilarityScoreMeasure<>(new JaroWinklerDistance());
    }

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

    public static SimilarityTransformation<String, String> colognePhoenitic() {
        return codec(new ColognePhonetic());
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
}
