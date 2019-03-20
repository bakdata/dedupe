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

import com.bakdata.dedupe.candidate_selection.CompositeValue;
import com.bakdata.dedupe.candidate_selection.SortingKey;
import com.bakdata.dedupe.candidate_selection.online.OnlineCandidateSelection;
import com.bakdata.dedupe.candidate_selection.online.OnlineSortedNeighborhoodMethod;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.regex.Pattern;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class PersonCandidateSelection implements OnlineCandidateSelection<Person> {
    public static final int WINDOW_SIZE = 20;
    private static final Pattern BRACED_TERMS = Pattern.compile("\\(.*?\\)");
    private static final Pattern NON_ALPHA = Pattern.compile("[^\\p{Alnum}]");
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

    private static String normalize(final String value) {
        if (value == null) {
            return null;
        }

        // split umlauts into canonicals
        // remove everything in braces
        // remove all non-alphanumericals
        final String nonBraced =
                BRACED_TERMS.matcher(Normalizer.normalize(value.toLowerCase(), Form.NFD)).replaceAll("");
        return NON_ALPHA.matcher(nonBraced).replaceAll("");
    }
}
