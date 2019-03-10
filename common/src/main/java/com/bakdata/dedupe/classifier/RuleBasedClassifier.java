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
 * <li>If no rule can be applied, the classification is {@link #defaultClassificationResult}.</li>
 * </ul>
 *
 * There are three types of rules:
 * <ul>
 * <li>Negatives rule are used to exclude false positives. For example, when two person records have completely
 * different social security numbers, we want to declare them non-duplicate, even if they share the same name.</li>
 * <li>A positive rule is used to explicitly include true positives. Positive rules can be used as exceptions of
 * negative rules or for very specific edge cases that are hard to model in a stable overall similarity measure. For
 * example, a person has changed the last name after marriage, so we exclude the last name of comparison and make
 * everything else stricter.</li>
 * <li>Threshold rules</li>
 * </ul>
 *
 * The {@code Classification} will contain a description naming the triggered rule and converts the rule score into a
 * confidence score.
 *
 * @param <T> the type of the record.
 */
@Value
@Builder
public class RuleBasedClassifier<T> implements Classifier<T> {
    /**
     * A value to signal that the rule cannot be applied (precondition failed).
     */
    private static final float DOES_NOT_APPLY = SimilarityMeasure.unknown();
    /**
     * The result when no rules could be applied.
     */
    private static final ClassificationResult UNKNOWN = ClassificationResult.builder()
            .confidence(0)
            .classification(Classification.UNKNOWN)
            .build();
    /**
     * The set of rules that are applied in the order of addition. The first rule that can be applied and gives a {@link
     * Classification} that is not unknown determines the {@link ClassificationResult}.
     */
    @Singular
    List<Rule<T>> rules;
    /**
     * Fallback value, when no rule applied. By default, a result with {@link Classification#UNKNOWN} is returned.
     */
    @Builder.Default
    ClassificationResult defaultClassificationResult = UNKNOWN;
    /**
     * Factory that creates the {@link SimilarityContext} before classifying an incoming {@link Candidate}.
     * <p>This factory could create a {@link SimilarityContext} with different null value handling.</p>
     */
    @NonNull
    @Builder.Default
    Supplier<SimilarityContext> contextSupplier = () -> SimilarityContext.builder().build();

    private static float scaleAtThreshold(float similarity, float threshold) {
        if (similarity >= threshold) {
            return (similarity - threshold) / (1 - threshold);
        }
        return -(threshold - similarity) / (threshold);
    }

    @Override
    public ClassificationResult classify(final Candidate<T> candidate) {
        final SimilarityContext context = contextSupplier.get();
        // find a rule that is applicable and gives a clear result
        ClassificationResult classificationResult = this.defaultClassificationResult;
        for (final Rule<T> rule : this.rules) {
            classificationResult = this.evaluateRule(rule, candidate, context).orElse(classificationResult);
            if (!classificationResult.getClassification().isUnknown()) {
                break;
            }
        }
        // check if we have one or more exceptions during rule applications
        if (!context.getExceptions().isEmpty()) {
            throw this.createException(candidate, context);
        }

        // if none of the rules applied, use default result
        return classificationResult.getClassification().isUnknown() ? this.defaultClassificationResult
                : classificationResult;
    }

    /**
     * Creates an exception with the first caught exception as root and all other exceptions as suppressed.
     */
    private SimilarityException createException(final Candidate<T> candidate, final SimilarityContext context) {
        final SimilarityException fusionException = new SimilarityException("Could not classify candidate " + candidate,
                context.getExceptions().get(0));
        context.getExceptions().stream().skip(1).forEach(fusionException::addSuppressed);
        return fusionException;
    }

    /**
     * Safely evaluates a rule for a candidate.
     */
    private Optional<ClassificationResult> evaluateRule(final Rule<? super T> rule, final Candidate<? extends T> candidate,
            final SimilarityContext context) {
        return context.safeExecute(() ->
                rule.evaluate(candidate.getRecord1(), candidate.getRecord2(), context))
                .map(score -> mapScoreToResult(rule, score));
    }

    /**
     * Maps the score to a result.
     * <p>A negative rule returns a negative score [-1; -0] and results in a NON_DUPLICATE.</p>
     * <p>A positive rule returns a positive score [0; 1] and results in a DUPLICATE.</p>
     */
    private ClassificationResult mapScoreToResult(Rule<? super T> rule, float score) {
        if (didNotApply(score)) {
            return UNKNOWN;
        }
        if (score <= -0.0f) {
            // negative rule gave [-1; -0], confidence is just negated
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
    }

    /**
     * Checks if score is equivalent to {@link #DOES_NOT_APPLY}.
     */
    private boolean didNotApply(float score) {
        return SimilarityMeasure.isUnknown(score);
    }

    /**
     * A builder for {@link RuleBasedClassifier} with convenience methods to create positive and negative rules.
     *
     * @param <T> the type of the record.
     */
    @SuppressWarnings({"WeakerAccess", "UnusedReturnValue"})
    public static class RuleBasedClassifierBuilder<T> {
        /**
         * Creates a positive rule that is applied only when the precondition holds. Following rules are only evaluated
         * if the similarity is either {@link SimilarityMeasure#unknown()} or 0. If the precondition fails, {@link
         * Rule#doesNotApply()} is returned and the following rules are evaluated.
         * <p>Positive rules result in {@link Classification#DUPLICATE}s.</p>
         *
         * @param name the name of the rule for lineage/debugging.
         * @param applicablePredicate a predicate for left and right input.
         * @param similarityMeasure the similarity measure of this rule.
         * @return this
         */
        public RuleBasedClassifierBuilder<T> positiveRule(final String name,
                final BiPredicate<T, T> applicablePredicate, final SimilarityMeasure<T> similarityMeasure) {
            return this.positiveRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? similarityMeasure.getSimilarity(left, right, context)
                            : DOES_NOT_APPLY);
        }

        /**
         * Creates a positive rule that is always applied. Following rules are only evaluated if the similarity is
         * either {@link SimilarityMeasure#unknown()} or 0.
         * <p>Positive rules result in {@link Classification#DUPLICATE}s.</p>
         *
         * @param name the name of the rule for lineage/debugging.
         * @param similarityMeasure the similarity measure of this rule.
         * @return this
         */
        public RuleBasedClassifierBuilder<T> positiveRule(final String name,
                final SimilarityMeasure<T> similarityMeasure) {
            return this.rule(new Rule<>(name, similarityMeasure.unknownIf(s -> s <= 0)));
        }

        /**
         * Creates a negative rule that is applied only when the precondition holds. Following rules are only evaluated
         * if the similarity is either {@link SimilarityMeasure#unknown()} or 0. If the precondition fails, {@link
         * Rule#doesNotApply()} is returned and the following rules are evaluated.
         * <p>Negative rules result in {@link Classification#NON_DUPLICATE}s.</p>
         *
         * @param name the name of the rule for lineage/debugging.
         * @param applicablePredicate a predicate for left and right input.
         * @param similarityMeasure the similarity measure of this rule.
         * @return this
         */
        public RuleBasedClassifierBuilder<T> negativeRule(final String name,
                final BiPredicate<T, T> applicablePredicate,
                final SimilarityMeasure<T> similarityMeasure) {
            return this.negativeRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? similarityMeasure.getSimilarity(left, right, context)
                            : DOES_NOT_APPLY);
        }

        /**
         * Creates a negative rule that is always applied. Following rules are only evaluated if the similarity is
         * either {@link SimilarityMeasure#unknown()} or 0.
         * <p>Negative rules result in {@link Classification#NON_DUPLICATE}s.</p>
         *
         * @param name the name of the rule for lineage/debugging.
         * @param similarityMeasure the similarity measure of this rule.
         * @return this
         */
        public RuleBasedClassifierBuilder<T> negativeRule(final String name,
                final SimilarityMeasure<? super T> similarityMeasure) {
            final SimilarityMeasure<T> negativeSim =
                    (left, right, context) -> -similarityMeasure.getSimilarity(left, right, context);
            return this.rule(new Rule<>(name, negativeSim.unknownIf(s -> s >= 0)));
        }

        /**
         * Creates a positive rule that is applied only when the precondition holds. Following rules are only evaluated
         * if the similarity is either {@link SimilarityMeasure#unknown()} or 0. If the precondition fails, {@link
         * Rule#doesNotApply()} is returned and the following rules are evaluated.
         * <p>Positive rules result in {@link Classification#DUPLICATE}s.</p>
         *
         * @param name the name of the rule for lineage/debugging.
         * @param applicablePredicate a predicate for left and right input.
         * @param similarityMeasure the similarity measure of this rule.
         * @return this
         */
        public RuleBasedClassifierBuilder<T> positiveRule(final String name,
                final BiPredicate<T, T> applicablePredicate, final SimilarityMeasure<T> similarityMeasure) {
            return this.positiveRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? similarityMeasure.getSimilarity(left, right, context)
                            : DOES_NOT_APPLY);
        }

        /**
         * Creates a positive rule that is always applied. Following rules are only evaluated if the similarity is
         * either {@link SimilarityMeasure#unknown()} or 0.
         * <p>Positive rules result in {@link Classification#DUPLICATE}s.</p>
         *
         * @param name the name of the rule for lineage/debugging.
         * @param similarityMeasure the similarity measure of this rule.
         * @return this
         */
        public RuleBasedClassifierBuilder<T> positiveRule(final String name,
                final SimilarityMeasure<T> similarityMeasure) {
            return this.rule(new Rule<>(name, similarityMeasure.unknownIf(s -> s <= 0)));
        }

        public RuleBasedClassifierBuilder<T> defaultRule(final SimilarityMeasure<T> similarityMeasure) {
            return this.rule(new Rule<>("default", similarityMeasure));
        }
    }

    /**
     * A rule has a name for lineage/debugging and the similarity measure.
     * <p>Note that through various factory methods, the {@link SimilarityMeasure} can be wrapped such that it returns
     * negative values.</p>
     * <p>Rules rarely need to be created manually. Please use the more expressive factory methods in {@link
     * RuleBasedClassifierBuilder}.</p>
     *
     * @param <T> the type of the record.
     */
    @Value
    public static class Rule<T> {
        /**
         * The name of the rule for lineage/debugging.
         */
        String name;
        /**
         * The similarity measure of this rule.
         * <p>Note that through various factory methods, the {@link SimilarityMeasure} can be wrapped such that it
         * returns negative values.</p>
         */
        SimilarityMeasure<T> measure;

        /**
         * Indicates that this similarity measure can not be applied (e.g., precondition not satisfied).
         */
        @SuppressWarnings("SameReturnValue")
        protected static float doesNotApply() {
            return DOES_NOT_APPLY;
        }

        /**
         * Calculates the similarity or returns {@link #doesNotApply()} of the similarity measure can not be applied.
         * <p>A negative value indicates a {@link Classification#NON_DUPLICATE} with negated confidence.</p>
         * <p>A non-negative value indicates a {@link Classification#DUPLICATE} where the value is the confidence.</p>
         * <p>A {@link #doesNotApply()} value indicates a {@link Classification#UNKNOWN} with a confidence of 0.</p>
         *
         * @param left the left element for which the similarity should be calculated.
         * @param right the right element for which the similarity should be calculated.
         * @param context the context of the comparison.
         * @return the similarity or {@link #doesNotApply()}.
         */
        float evaluate(final T left, final T right, final SimilarityContext context) {
            return this.measure.getSimilarity(left, right, context);
        }
    }
}
