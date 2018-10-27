package com.bakdata.deduplication.person;

import com.bakdata.deduplication.candidate_selection.CompositeValue;
import com.bakdata.deduplication.candidate_selection.SortingKey;
import com.bakdata.deduplication.candidate_selection.online.OnlineCandidateSelection;
import com.bakdata.deduplication.candidate_selection.online.OnlineSortedNeighborhoodMethod;
import lombok.Value;
import lombok.experimental.Delegate;

import java.text.Normalizer;

@Value
public class PersonCandidateSelection implements OnlineCandidateSelection<Person> {
    static int WINDOW_SIZE = 20;
    @Delegate
    OnlineCandidateSelection<Person> candidateSelection = OnlineSortedNeighborhoodMethod.<Person>builder()
            .defaultWindowSize(WINDOW_SIZE)
            .sortingKey(new SortingKey<>("First name+Last name",
                    person -> CompositeValue.of(normalize(person.getFirstName()), normalize(person.getLastName()))))
            .sortingKey(new SortingKey<>("Last name+First name",
                    person -> CompositeValue.of(normalize(person.getLastName()), normalize(person.getFirstName()))))
            .sortingKey(new SortingKey<>("Bday+Last name",
                    person -> CompositeValue.of(person.getBirthDate(), normalize(person.getLastName()))))
            .build();

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }

        // split umlauts into canonicals
        return Normalizer.normalize(value.toLowerCase(), Normalizer.Form.NFD)
                // remove everything in braces
                .replaceAll("\\(.*?\\)", "")
                // remove all non-alphanumericals
                .replaceAll("[^\\p{Alnum}]", "");
    }
}
