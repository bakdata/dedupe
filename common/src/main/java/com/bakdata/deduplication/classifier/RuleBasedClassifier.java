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
package com.bakdata.deduplication.classifier;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.similarity.SimilarityContext;
import com.bakdata.deduplication.similarity.SimilarityException;
import com.bakdata.deduplication.similarity.SimilarityMeasure;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * Successively applies a list of rules to the record and returns the respective {@link Classification} with the following cases:
 * <li>
 * <ul>If any rule classifies the pair unambiguously as {@link Classification.ClassificationResult#DUPLICATE} or {@link Classification.ClassificationResult#NON_DUPLICATE}, the classification is immediately returned.</ul>
 * <ul>If some rule classifies the pair as {@link Classification.ClassificationResult#POSSIBLE_DUPLICATE}, the remaining rules with be evaluated to see if an unambiguous classification will be reached, in which case that classification is returned. If the results are only ambiguous, the last {@code POSSIBLE_DUPLICATE} classification will be returned.</ul>
 * <ul>If no rule can be applied, the result is {@link #UNKNOWN}.</ul>
 * </li>
 * <p>
 * The {@code Classification} will contain a description naming the triggered rule and converts the rule score into a confidence score.
 *
 * @param <T>
 */
@Value
@Builder
public class RuleBasedClassifier<T> implements Classifier<T> {
    public static final float DOES_NOT_APPLY = Float.NaN;
    public static final Classification UNKNOWN = Classification.builder()
            .confidence(0)
            .result(Classification.ClassificationResult.UNKNOWN)
            .build();
    @Singular
    List<Rule<T>> rules;
    @Builder.Default
    Classification defaultClassification = UNKNOWN;

    @Override
    public Classification classify(Candidate<T> candidate) {
        SimilarityContext context = new SimilarityContext();
        var classification = defaultClassification;
        for (Rule<T> rule : rules) {
            classification = evaluateRule(rule, candidate, context).orElse(classification);
            if (!classification.getResult().isAmbiguous()) {
                break;
            }
        }
        if (!context.getExceptions().isEmpty()) {
            throw createException(candidate, context);
        }
        return classification;
    }

    private SimilarityException createException(Candidate<T> candidate, SimilarityContext context) {
        final SimilarityException fusionException = new SimilarityException("Could not classify candidate " + candidate,
                context.getExceptions().get(0));
        context.getExceptions().stream().skip(1).forEach(fusionException::addSuppressed);
        return fusionException;
    }

    private Optional<Classification> evaluateRule(Rule<T> rule, Candidate<T> candidate, SimilarityContext context) {
        return context.safeExecute(() -> rule.evaluate(candidate.getNewRecord(), candidate.getOldRecord(), context)).map(score -> {
            if (Float.isNaN(score)) {
                return UNKNOWN;
            }
            if (score <= -0f) {
                return Classification.builder()
                        .result(Classification.ClassificationResult.NON_DUPLICATE)
                        .confidence(-score)
                        .explanation(rule.getName())
                        .build();
            } else {
                return Classification.builder()
                        .result(Classification.ClassificationResult.DUPLICATE)
                        .confidence(score)
                        .explanation(rule.getName())
                        .build();
            }
        });
    }

    @SuppressWarnings({"WeakerAccess", "UnusedReturnValue"})
    public static class RuleBasedClassifierBuilder<T> {

        public RuleBasedClassifierBuilder<T> positiveRule(String name, BiPredicate<T, T> applicablePredicate,
                                                          SimilarityMeasure<T> similarityMeasure) {
            return positiveRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? similarityMeasure.getSimilarity(left, right, context) : DOES_NOT_APPLY);
        }

        public RuleBasedClassifierBuilder<T> positiveRule(String name, SimilarityMeasure<T> similarityMeasure) {
            return rule(new Rule<>(name, similarityMeasure.unknownIf(s -> s <= 0)));
        }

        public RuleBasedClassifierBuilder<T> negativeRule(String name, BiPredicate<T, T> applicablePredicate,
                                                          SimilarityMeasure<T> similarityMeasure) {
            return negativeRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? similarityMeasure.getSimilarity(left, right, context) : DOES_NOT_APPLY);
        }

        public RuleBasedClassifierBuilder<T> negativeRule(String name, SimilarityMeasure<T> similarityMeasure) {
            final SimilarityMeasure<T> negativeSim = (left, right, context) -> -similarityMeasure.getSimilarity(left, right, context);
            return rule(new Rule<>(name, negativeSim.unknownIf(s -> s >= 0)));
        }

        public RuleBasedClassifierBuilder<T> defaultResult(Classification.ClassificationResult result) {
            return defaultClassification(Classification.builder()
                    .confidence(0)
                    .result(result)
                    .build());
        }
    }

    @Value
    public static class Rule<T> {
        String name;
        SimilarityMeasure<T> measure;

        float evaluate(T left, T right, SimilarityContext context) {
            return measure.getSimilarity(left, right, context);
        }

        @SuppressWarnings("SameReturnValue")
        protected float doesNotApply() {
            return DOES_NOT_APPLY;
        }
    }
}