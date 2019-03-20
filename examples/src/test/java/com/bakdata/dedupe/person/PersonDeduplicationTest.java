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

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.dedupe.duplicate_detection.PossibleDuplicateHandler;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.jupiter.api.Test;

class PersonDeduplicationTest {
    private static final DateTimeFormatter BDAY_FORMAT = DateTimeFormatter.ofPattern("dd.MM.yy");

    private static List<Person> parseCsv(final String resourceName) throws IOException {
        final CSVFormat format = CSVFormat.newFormat('\t').withFirstRecordAsHeader().withQuote('"');
        try (final CSVParser parser = CSVParser
                .parse(PersonDeduplicationTest.class.getResourceAsStream(resourceName), StandardCharsets.UTF_8,
                        format)) {
            return parser.getRecords()
                    .stream()
                    .map(record -> Person.builder()
                            .id(record.get("id"))
                            .firstName(record.get("firstname_full"))
                            .lastName(record.get("lastname"))
                            .birthDate(LocalDate.parse(record.get("birthdate"), BDAY_FORMAT))
                            .gender(Gender.valueOf(record.get("gender").toUpperCase()))
                            .lastModified(LocalDateTime.now())
                            .build())
                    .collect(Collectors.toList());
        }
    }

    @Test
    void testDeduplication() throws IOException {
        final PersonDeduplication deduplication = new PersonDeduplication(PossibleDuplicateHandler.keep(), Optional::of);

        // no fusion on the non-duplicated customers
        for (final Person customer : parseCsv("/customer.csv")) {
            final Person fusedPerson = deduplication.deduplicate(customer);
            assertThat(fusedPerson).isSameAs(customer);
        }

        for (final Person customer : parseCsv("/exact_duplicates.csv")) {
            final Person fusedPerson = deduplication.deduplicate(customer);
            assertThat(fusedPerson).isNotSameAs(customer);
            // should be the same except for fusion id
            assertThat(fusedPerson.toBuilder().fusedIds(Set.of()).build()).isEqualTo(customer);
        }
    }
}
