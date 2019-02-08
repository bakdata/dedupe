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
    public static final DateTimeFormatter ISO_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

    @Delegate
    Classifier<Person> classifier = RuleBasedClassifier.<Person>builder()
            .positiveRule("Basic comparison", CommonSimilarityMeasures.<Person>weightedAverage()
                    .add(2, Person::getFirstName, max(levenshtein().cutoff(0.5f), jaroWinkler()))
                    .add(2, Person::getLastName, max(equality().of(colognePhonetic()), levenshtein().cutoff(0.5f), jaroWinkler()))
                    .add(1, Person::getGender, equality())
                    .add(2, Person::getBirthDate, max(levenshtein().of(ISO_FORMAT::format), maxDiff(2, ChronoUnit.DAYS)))
                    .build()
                    .scaleWithThreshold(0.9f))
            .build();
}
