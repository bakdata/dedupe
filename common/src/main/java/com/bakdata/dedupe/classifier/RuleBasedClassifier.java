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
package com.bakdata.dedupe.classifier;

import com.bakdata.dedupe.candidate_selection.Candidate;
import com.bakdata.dedupe.similarity.SimilarityContext;
import com.bakdata.dedupe.similarity.SimilarityException;
import com.bakdata.dedupe.similarity.SimilarityMeasure;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

/**
 * Successively applies a list of rules to the record and returns the respective {@link ClassificationResult} with the
 * following cases:
 * <ul>
 * <li>If any rule classifies the pair unambiguously as {@link Classification#DUPLICATE} or {@link
 * Classification#NON_DUPLICATE}, the classification is immediately returned.</li>
 * <li>If some rule classifies the pair as {@link Classification#POSSIBLE_DUPLICATE}, the
 * remaining
 * rules with be evaluated to see if an unambiguous classification will be reached, in which case that classification is
 * returned. If the results are only ambiguous, the last {@code POSSIBLE_DUPLICATE} classification will be
 * returned.</li>
 * <li>If no rule can be applied, the duplicateClass is {@link #UNKNOWN}.</li>
 * </ul>
 * <br>
 * The {@code Classification} will contain a description naming the triggered rule and converts the rule score into a
 * confidence score.
 */
@Value
@Builder
public class RuleBasedClassifier<T> implements Classifier<T> {
    public static final float DOES_NOT_APPLY = Float.NaN;
    public static final ClassificationResult UNKNOWN = ClassificationResult.builder()
            .confidence(0)
            .classification(Classification.UNKNOWN)
            .build();
    @Singular
    List<Rule<T>> rules;
    @Builder.Default
    ClassificationResult defaultClassificationResult = UNKNOWN;
    @NonNull
    @Builder.Default
    Supplier<SimilarityContext> contextSupplier = () -> SimilarityContext.builder().build();

    @Override
    public ClassificationResult classify(final Candidate<T> candidate) {
        final SimilarityContext context = contextSupplier.get();
        ClassificationResult classificationResult = this.defaultClassificationResult;
        for (final Rule<T> rule : this.rules) {
            classificationResult = this.evaluateRule(rule, candidate, context).orElse(classificationResult);
            if (!classificationResult.getClassification().isAmbiguous()) {
                break;
            }
        }
        if (!context.getExceptions().isEmpty()) {
            throw this.createException(candidate, context);
        }
        return classificationResult.getClassification().isAmbiguous() ? this.defaultClassificationResult
                : classificationResult;
    }

    private SimilarityException createException(final Candidate<T> candidate, final SimilarityContext context) {
        final SimilarityException fusionException = new SimilarityException("Could not classify candidate " + candidate,
                context.getExceptions().get(0));
        context.getExceptions().stream().skip(1).forEach(fusionException::addSuppressed);
        return fusionException;
    }

    private Optional<ClassificationResult> evaluateRule(final Rule<? super T> rule, final Candidate<? extends T> candidate,
            final SimilarityContext context) {
        return context.safeExecute(() -> rule.evaluate(candidate.getRecord1(), candidate.getRecord2(), context))
                .map(score -> {
                    if (Float.isNaN(score)) {
                        return UNKNOWN;
                    }
                    if (score <= -0.0f) {
                        return ClassificationResult.builder()
                                .classification(Classification.NON_DUPLICATE)
                                .confidence(-score)
                                .explanation(rule.getName())
                                .build();
                    } else {
                        return ClassificationResult.builder()
                                .classification(Classification.DUPLICATE)
                                .confidence(score)
                                .explanation(rule.getName())
                                .build();
                    }
                });
    }

    @SuppressWarnings({"WeakerAccess", "UnusedReturnValue"})
    public static class RuleBasedClassifierBuilder<T> {

        public RuleBasedClassifierBuilder<T> positiveRule(final String name,
                final BiPredicate<T, T> applicablePredicate,
                final SimilarityMeasure<T> similarityMeasure) {
            return this.positiveRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? similarityMeasure.getSimilarity(left, right, context)
                            : DOES_NOT_APPLY);
        }

        public RuleBasedClassifierBuilder<T> positiveRule(final String name,
                final SimilarityMeasure<T> similarityMeasure) {
            return this.rule(new Rule<>(name, similarityMeasure.unknownIf(s -> s <= 0)));
        }

        public RuleBasedClassifierBuilder<T> negativeRule(final String name,
                final BiPredicate<T, T> applicablePredicate,
                final SimilarityMeasure<T> similarityMeasure) {
            return this.negativeRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? similarityMeasure.getSimilarity(left, right, context)
                            : DOES_NOT_APPLY);
        }

        public RuleBasedClassifierBuilder<T> negativeRule(final String name,
                final SimilarityMeasure<? super T> similarityMeasure) {
            final SimilarityMeasure<T> negativeSim =
                    (left, right, context) -> -similarityMeasure.getSimilarity(left, right, context);
            return this.rule(new Rule<>(name, negativeSim.unknownIf(s -> s >= 0)));
        }

        public RuleBasedClassifierBuilder<T> defaultRule(final SimilarityMeasure<T> similarityMeasure) {
            return this.rule(new Rule<>("default", similarityMeasure));
        }
    }

    @Value
    public static class Rule<T> {
        String name;
        SimilarityMeasure<T> measure;

        @SuppressWarnings("SameReturnValue")
        protected static float doesNotApply() {
            return DOES_NOT_APPLY;
        }

        float evaluate(final T left, final T right, final SimilarityContext context) {
            return this.measure.getSimilarity(left, right, context);
        }
    }
}
