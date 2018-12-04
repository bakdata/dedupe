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

import com.bakdata.deduplication.fusion.ConflictResolution;
import com.bakdata.deduplication.fusion.ConflictResolutionFusion;
import com.bakdata.deduplication.fusion.ConflictResolutions;
import com.bakdata.deduplication.fusion.Fusion;
import lombok.Value;
import lombok.experimental.Delegate;

import java.util.Set;

import static com.bakdata.deduplication.fusion.CommonConflictResolutions.*;

@Value
public class PersonFusion implements Fusion<Person> {
    ConflictResolution<Person, Person> personMerge = ConflictResolutions.merge(Person::new)
            .field(Person::getId, Person::setId).with(min())
            .field(Person::getFirstName, Person::setFirstName).with(longest()).then(vote())
            .field(Person::getLastName, Person::setLastName).correspondingToPrevious()
            .field(Person::getGender, Person::setGender).with(assumeEqualValue())
            .field(Person::getBirthDate, Person::setBirthDate).with(vote()).then(latest())
            .field(Person::getLastModified, Person::setLastModified).with(max())
            .field(PersonFusion::fusionIdWithPersonId, Person::setFusedIds).with(union())
            .build();
    @Delegate
    Fusion<Person> fusion = ConflictResolutionFusion.<Person>builder()
            .sourceExtractor(Person::getSource)
            .lastModifiedExtractor(Person::getLastModified)
            .rootResolution(personMerge)
            .build();

    private static Set<String> fusionIdWithPersonId(Person p) {
        if (p.getFusedIds().isEmpty()) {
            return Set.of(p.getId());
        }
        return p.getFusedIds();
    }
}
