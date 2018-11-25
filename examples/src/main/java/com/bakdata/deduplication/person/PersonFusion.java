package com.bakdata.deduplication.person;

import com.bakdata.deduplication.fusion.ConflictResolution;
import com.bakdata.deduplication.fusion.ConflictResolutionFusion;
import com.bakdata.deduplication.fusion.ConflictResolutions;
import com.bakdata.deduplication.fusion.Fusion;
import com.google.common.collect.Sets;
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

    private static Set<String> fusionIdWithPersonId(Person p) {
        if(p.getFusedIds().isEmpty()) {
            return Set.of(p.id);
        }
        return p.fusedIds;
    }

    @Delegate
    Fusion<Person> fusion = ConflictResolutionFusion.<Person>builder()
            .sourceExtractor(Person::getSource)
            .lastModifiedExtractor(Person::getLastModified)
            .rootResolution(personMerge)
            .build();
}
