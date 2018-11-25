package com.bakdata.deduplication.person;

import com.bakdata.deduplication.classifier.Classifier;
import com.bakdata.deduplication.classifier.RuleBasedClassifier;
import com.bakdata.deduplication.similarity.CommonSimilarityMeasures;
import com.bakdata.deduplication.similarity.SimilarityMeasure;
import lombok.Value;
import lombok.experimental.Delegate;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bakdata.deduplication.similarity.CommonSimilarityMeasures.*;

@Value
public class FrequencyAwarePersonClassifier implements Classifier<Person> {
    static DateTimeFormatter ISO_FORMAT = DateTimeFormatter.ISO_DATE;
    static float THRESHOLD = .9f;

    @Delegate
    Classifier<Person> classifier;

    public FrequencyAwarePersonClassifier() {
        var classifierBuilder = RuleBasedClassifier.<Person>builder();

        addNameFrequencyRules(classifierBuilder);

        // no weight on last name and stricter threshold
        classifierBuilder.positiveRule("last name change", createSimilarityMeasure(2, 0, (1 + THRESHOLD) / 2));

        this.classifier = classifierBuilder.build();
    }

    private void addNameFrequencyRules(RuleBasedClassifier.RuleBasedClassifierBuilder<Person> classifierBuilder) {
        @Value
        class Names {
            String label;
            float weight;
            Predicate<String> predicate;

            boolean containsAny(String name1, String name2) {
                return predicate.test(name1.toLowerCase()) || predicate.test(name2.toLowerCase());
            }
        }

        List<Names> surnameVariations = List.of(
            new Names("most common surname", 1, readNames("/most_common_surnames_us.txt")::contains),
            new Names("medium common surname", 2, readNames("/medium_common_surnames_us.txt")::contains),
            new Names("rare surname", 3, (name) -> true));
        List<Names> givennameVariations = List.of(
            new Names("most common given name", 1, readNames("/most_common_givennames_us.txt")::contains),
            new Names("medium common given name", 2, readNames("/medium_common_givennames_us.txt")::contains),
            new Names("rare given name", 3, (name) -> true));
        for (Names surnames : surnameVariations) {
            for (Names givennames : givennameVariations) {
                classifierBuilder.positiveRule(surnames.getLabel() + " - " + givennames.getLabel(),
                    ((person1, person2) -> surnames.containsAny(person1.getLastName(), person2.getLastName()) &&
                        givennames.containsAny(person1.getFirstName(), person2.getFirstName())),
                    createSimilarityMeasure(givennames.getWeight(), surnames.getWeight(), THRESHOLD));
            }
        }
    }

    private static SimilarityMeasure<Person> createSimilarityMeasure(float firstNameWeight, float lastNameWeight, float threshold) {
        return CommonSimilarityMeasures.<Person>weightedAverage()
            .add(firstNameWeight, Person::getFirstName, max(levenshtein().cutoff(.5f), jaroWinkler()))
            .add(lastNameWeight, Person::getLastName, max(equality().of(colognePhoneitic()), levenshtein().cutoff(.5f), jaroWinkler()))
            .add(1, Person::getGender, equality())
            .add(2, Person::getBirthDate, max(levenshtein().of(ISO_FORMAT::format), maxDiff(2, ChronoUnit.DAYS)))
            .build()
            .scaleWithThreshold(threshold);
    }

    private static Set<String> readNames(String s) {
        try(final Stream<String> lines = Files.lines(Paths.get(FrequencyAwarePersonClassifier.class.getResource(s).toURI()))) {
            return lines.map(String::toLowerCase)
                .collect(Collectors.toSet());
        } catch (IOException | URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
