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

import static com.bakdata.dedupe.fusion.CommonConflictResolutions.assumeEqualValue;
import static com.bakdata.dedupe.fusion.CommonConflictResolutions.latest;
import static com.bakdata.dedupe.fusion.CommonConflictResolutions.longest;
import static com.bakdata.dedupe.fusion.CommonConflictResolutions.max;
import static com.bakdata.dedupe.fusion.CommonConflictResolutions.merge;
import static com.bakdata.dedupe.fusion.CommonConflictResolutions.min;
import static com.bakdata.dedupe.fusion.CommonConflictResolutions.union;
import static com.bakdata.dedupe.fusion.CommonConflictResolutions.vote;

import com.bakdata.dedupe.fusion.ConflictResolution;
import com.bakdata.dedupe.fusion.ConflictResolutionFusion;
import com.bakdata.dedupe.fusion.Fusion;
import java.util.Set;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class PersonFusion implements Fusion<Person> {
    ConflictResolution<Person, Person> personMerge = merge(Person::new)
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
            .rootResolution(this.personMerge)
            .build();

    private static Set<String> fusionIdWithPersonId(final Person p) {
        if (p.getFusedIds().isEmpty()) {
            return Set.of(p.getId());
        }
        return p.getFusedIds();
    }
}
