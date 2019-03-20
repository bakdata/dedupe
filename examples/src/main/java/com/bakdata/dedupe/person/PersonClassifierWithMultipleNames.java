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
package com.bakdata.dedupe.person;

import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.equality;
import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.jaroWinkler;
import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.levenshtein;
import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.max;
import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.scaledDifference;
import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.stableMatching;
import static com.bakdata.dedupe.similarity.CommonTransformations.beiderMorse;
import static com.bakdata.dedupe.similarity.CommonTransformations.words;

import com.bakdata.dedupe.classifier.Classifier;
import com.bakdata.dedupe.classifier.RuleBasedClassifier;
import com.bakdata.dedupe.similarity.CommonSimilarityMeasures;
import com.bakdata.dedupe.similarity.SimilarityMeasure;
import com.bakdata.dedupe.similarity.ValueTransformation;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import lombok.experimental.Delegate;

/**
 * Shows a classifier for data points that have been incorrectly reassigned.
 * <p>In this case, we assume that the first and last name has not been correctly split in some pre-processing step and
 * we have the following cases for some person Edgar Allan Poe</p>
 * <ol>
 * <li>All correct: first=Edgar Allan, last=Poe</li>
 * <li>Abbreviated: first=Edgar A., last=Poe</li>
 * <li>Incorrect split: first=Edgar, last=Allan Poe</li>
 * <li>Swapped: first=Poe, last=Edgar Allan</li>
 * <li>Scrambled: first=Allan Poe, last=Edgar</li>
 * </ol>
 *
 * This example addresses these issues, but please verify during an evaluation that the similarity measure does not get
 * too weak. Usually, a few missed duplicates are not as bad as some wrongly classified non-duplicates!
 */
@Value
public class PersonClassifierWithMultipleNames implements Classifier<Person> {
    public static final DateTimeFormatter ISO_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    /**
     * Case 1 and case 2: correctly split, maybe abbreviated.
     * <p>Abbreviation is indirectly handled through Jaro-Winkler. If abbreviation is very common, consider using a
     * custom similarity measure.</p>
     */
    SimilarityMeasure<Person> namesCorrectlySplitSimilarity = CommonSimilarityMeasures.<Person>weightedAverage()
            .add(1, Person::getFirstName, max(levenshtein().cutoff(0.9d), jaroWinkler()))
            .add(1, Person::getLastName, max(equality().of(beiderMorse()), levenshtein().cutoff(0.8d), jaroWinkler()))
            .build();

    /**
     * Case 3: To handle incorrect split, we concatenate first and last and perform a holistic match with edit
     * distance.
     * <p>Note that we need to be stricter (=higher threshold) than {@link #namesCorrectlySplitSimilarity} to avoid too
     * many false positives.</p>
     */
    SimilarityMeasure<Person> namesIncorrectlySplitSimilarity = levenshtein().cutoff(0.9d).of(
            PersonClassifierWithMultipleNames.concatNames());

    /**
     * Case 4: Swapped first and last name.
     * <p>nameSimilarity combines the strictest version of first and last name measures.</p>
     * <p>stableMatching on combined names list will match items only once.</p>
     * <p></p>
     * <p>However, mongeElkan does not work for names, where first an last names are similar because mongeElkan can
     * match the right hand side several times.</p>
     * <p>{@literal Example: John Johnson and John Smith will match 1) John -> John = 1.0; 2) Johnson -> John = 0.91}
     * </p>
     */
    SimilarityMeasure<String> nameSimilarity =
            max(levenshtein().cutoff(0.9d), jaroWinkler(), equality().of(beiderMorse()));
    SimilarityMeasure<Person> namesSwappedSimilarity = stableMatching(this.nameSimilarity)
            .of(person -> List.of(person.getFirstName(), person.getLastName()));

    /**
     * Case 5: Scrambled names, which first concatenates names and then splits then into bags of words.
     * <p>stableMatching on combined names list will match items only once.</p>
     * <p>Currently subsumes namesSwappedSimilarity but should be made stricter.</p>
     * <p></p>
     * <p>However, mongeElkan does not work for names, where first an last names are similar because mongeElkan can
     * match the right hand side several times.</p>
     * <p>{@literal Example: John Johnson and John Smith will match 1) John -> John = 1.0; 2) Johnson -> John = 0.91}
     * </p>
     */
    SimilarityMeasure<Person> namesScrambledSimilarity = stableMatching(this.nameSimilarity)
            .of(concatNames().andThen(words()));

    SimilarityMeasure<Person> overallNameSimilarity = max(this.namesCorrectlySplitSimilarity,
            this.namesIncorrectlySplitSimilarity,
            this.namesSwappedSimilarity,
            this.namesScrambledSimilarity);

    @Delegate
    Classifier<Person> classifier = RuleBasedClassifier.<Person>builder()
            .positiveRule("Basic comparison", CommonSimilarityMeasures.<Person>weightedAverage()
                    .add(4, this.overallNameSimilarity)
                    .add(1, Person::getGender, equality())
                    .add(2, Person::getBirthDate,
                            max(levenshtein().of(ISO_FORMAT::format), scaledDifference(2, ChronoUnit.DAYS)))
                    .build()
                    .scaleWithThreshold(0.9d))
            .build();

    private static ValueTransformation<Person, CharSequence> concatNames() {
        return (person, context) -> Stream.of(person.getFirstName(), person.getLastName())
                .filter(Objects::nonNull)
                .collect(Collectors.joining(" "));
    }
}
