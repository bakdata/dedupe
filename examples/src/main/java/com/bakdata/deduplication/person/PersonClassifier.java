package com.bakdata.deduplication.person;

import com.bakdata.deduplication.classifier.Classifier;
import com.bakdata.deduplication.classifier.RuleBasedClassifier;
import com.bakdata.deduplication.similarity.CommonSimilarityMeasures;
import lombok.Value;
import lombok.experimental.Delegate;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static com.bakdata.deduplication.similarity.CommonSimilarityMeasures.*;

@Value
public class PersonClassifier implements Classifier<Person> {
    static DateTimeFormatter ISO_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    @Delegate
    Classifier<Person> classifier = RuleBasedClassifier.<Person>builder()
            .positiveRule("Basic comparison", CommonSimilarityMeasures.<Person>weightedAverage()
                    .add(2, Person::getFirstName, max(levenshtein().cutoff(.5f), jaroWinkler()))
                    .add(2, Person::getLastName, max(equality().of(colognePhonetic()), levenshtein().cutoff(.5f), jaroWinkler()))
                    .add(1, Person::getGender, equality())
                    .add(2, Person::getBirthDate, max(levenshtein().of(ISO_FORMAT::format), maxDiff(2, ChronoUnit.DAYS)))
                    .build()
                    .scaleWithThreshold(.9f))
            .build();
}
