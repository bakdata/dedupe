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
 *     <ul>If any rule classifies the pair unambiguously as {@link Classification.ClassificationResult#DUPLICATE} or {@link Classification.ClassificationResult#NON_DUPLICATE}, the classification is immediately returned.</ul>
 *     <ul>If some rule classifies the pair as {@link Classification.ClassificationResult#POSSIBLE_DUPLICATE}, the remaining rules with be evaluated to see if an unambiguous classification will be reached, in which case that classification is returned. If the results are only ambiguous, the last {@code POSSIBLE_DUPLICATE} classification will be returned.</ul>
 *     <ul>If no rule can be applied, the result is {@link #UNKNOWN}.</ul>
 * </li>
 *
 * The {@code Classification} will contain a description naming the triggered rule and converts the rule score into a confidence score.
 *
 * @param <T>
 */
@Value
@Builder
public class RuleBasedClassifier<T> implements Classifier<T> {
    static float DOES_NOT_APPLY = Float.NaN;
    static Classification UNKNOWN = Classification.builder()
            .confidence(0)
            .result(Classification.ClassificationResult.UNKNOWN)
            .build();
    @Singular
    List<Rule<T>> rules;

    @Override
    public Classification classify(Candidate<T> candidate) {
        SimilarityContext context = new SimilarityContext();
        var classification = UNKNOWN;
        for (Rule<T> rule : rules) {
            Optional<Classification> optClassification = evaluateRule(rule, candidate, context);
            if (optClassification.map(cl -> !cl.getResult().isUnknown()).orElse(false)) {
                classification = optClassification.get();
                if (!classification.getResult().isAmbiguous()) {
                    break;
                }
            }
        }
        if(!context.getExceptions().isEmpty()) {
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
            if(Float.isNaN(score)) {
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
            return rule(new Rule<>(name, similarityMeasure));
        }

        public RuleBasedClassifierBuilder<T> negativeRule(String name, BiPredicate<T, T> applicablePredicate,
                                                          SimilarityMeasure<T> similarityMeasure) {
            return negativeRule(name, (left, right, context) ->
                    applicablePredicate.test(left, right) ? -similarityMeasure.getSimilarity(left, right, context) : DOES_NOT_APPLY);
        }

        public RuleBasedClassifierBuilder<T> negativeRule(String name, SimilarityMeasure<T> similarityMeasure) {
            return rule(new Rule<>(name, (left, right, context) -> -similarityMeasure.getSimilarity(left, right, context)));
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