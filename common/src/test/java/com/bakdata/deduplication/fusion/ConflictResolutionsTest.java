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
package com.bakdata.deduplication.fusion;

import static com.bakdata.deduplication.fusion.CommonConflictResolutions.assumeEqualValue;
import static com.bakdata.deduplication.fusion.CommonConflictResolutions.latest;
import static com.bakdata.deduplication.fusion.CommonConflictResolutions.longest;
import static com.bakdata.deduplication.fusion.CommonConflictResolutions.max;
import static com.bakdata.deduplication.fusion.CommonConflictResolutions.min;
import static com.bakdata.deduplication.fusion.CommonConflictResolutions.union;
import static com.bakdata.deduplication.fusion.CommonConflictResolutions.vote;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.bakdata.deduplication.fusion.Merge.AdditionalFieldMergeBuilder;
import com.bakdata.deduplication.fusion.Merge.FieldMergeBuilder;
import com.bakdata.deduplication.fusion.Merge.MergeBuilder;
import com.bakdata.util.FunctionalClass;
import com.google.common.collect.Sets;
import java.beans.IntrospectionException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.FieldDefaults;
import org.junit.jupiter.api.Test;

class ConflictResolutionsTest {

    private static MergeBuilder<Person> create() {
        return ConflictResolutions.merge(Person::new);
    }

    private static AdditionalFieldMergeBuilder<String, Person> createWithId() {
        return create()
                .field(Person::getId, Person::setId).with(min());
    }

    private static Set<String> fusionIdWithPersonId(final Person p) {
        return Set.copyOf(Sets.union(p.getFusedIds(), Set.of(p.getId())));
    }

    private static void testField(final FieldMergeBuilder<String, Person> field) {
        final FieldMergeBuilder<String, Person> nestedField = field.with(min())
                .field(Person::getFirstName, Person::setFirstName);
        testNestedField(nestedField);
    }

    private static void testMerge(final MergeBuilder<Person> merge) {
        final FieldMergeBuilder<String, Person> field = merge.field(Person::getId, Person::setId);
        testField(field);
    }

    private static void testNestedField(final FieldMergeBuilder<String, Person> nestedField) {
        final ConflictResolution<Person, Person> resolution = nestedField.with(longest()).then(vote())
                .field(Person::getLastName, Person::setLastName).correspondingToPrevious()
                .field(Person::getGender, Person::setGender).with(assumeEqualValue())
                .field(Person::getBirthDate, Person::setBirthDate).with(vote()).then(latest())
                .field(Person::getLastModified, Person::setLastModified).with(max())
                .field(ConflictResolutionsTest::fusionIdWithPersonId, Person::setFusedIds).with(union())
                .build();
        testResolution(resolution);
    }

    private static void testResolution(final ConflictResolution<Person, Person> resolution) {
        assertThat(resolution)
                .isNotNull();
        final FusionContext context = new FusionContext();
        final Person person1 = Person.builder()
                .id("id1")
                .firstName("Joanna")
                .lastName("Doe")
                .gender(Gender.FEMALE)
                .birthDate(LocalDate.of(2017, Month.DECEMBER, 31))
                .lastModified(LocalDateTime.MAX)
                .build();
        final Source source1 = new Source("source1", 1.0f);
        final LocalDateTime dateTime = LocalDateTime.MIN;
        final Person person2 = Person.builder()
                .id("id2")
                .firstName("John")
                .lastName("Smith")
                .gender(Gender.MALE)
                .birthDate(LocalDate.of(2018, Month.JANUARY, 1))
                .lastModified(LocalDateTime.MIN)
                .build();
        final Source source2 = new Source("source2", 2.0f);
        final List<AnnotatedValue<Person>> values = List.of(
                new AnnotatedValue<>(person1, source1, dateTime),
                new AnnotatedValue<>(person2, source2, dateTime));
        final Optional<Person> resolved = resolution.resolve(values, context);
        final Person expected = Person.builder()
                .id("id1")
                .firstName("Joanna")
                .lastName("Doe")
                .gender(null)
                .birthDate(LocalDate.of(2018, Month.JANUARY, 1))
                .lastModified(LocalDateTime.MAX)
                .fusedIds(Set.of("id1", "id2"))
                .build();
        assertThat(resolved)
                .isNotEmpty()
                .hasValue(expected);
        final List<Exception> exceptions = context.getExceptions();
        assertThat(exceptions)
                .hasSize(1);
        assertThat(exceptions.get(0))
                .isInstanceOf(FusionException.class)
                .hasMessageContaining(Gender.FEMALE.toString())
                .hasMessageContaining(Gender.MALE.toString())
                .hasMessageContaining("Could not fully resolve");
    }

    @Test
    void testFieldFromField() {
        final FieldMergeBuilder<String, Person> field = create()
                .field(FunctionalClass.from(Person.class).field("id"));
        testField(field);
    }

    @Test
    void testFieldFromGetterSetter() {
        final FieldMergeBuilder<String, Person> field = create()
                .field(Person::getId, Person::setId);
        testField(field);
    }

    @Test
    void testFieldFromName() {
        final FieldMergeBuilder<String, Person> field = create()
                .field("id");
        testField(field);
    }

    @Test
    void testFieldFromNameWithoutGetter() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> ConflictResolutions.merge(PersonWithoutGetter::new).field("id"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Method not found: isId");
    }

    @Test
    void testFieldFromNameWithoutSetter() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> ConflictResolutions.merge(PersonWithoutSetter::new).field("id"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Method not found: setId");
    }

    @Test
    void testFieldFromWrongField() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> create().field(FunctionalClass.from(Person.class).field("i")))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Method not found: isI");
    }

    @Test
    void testFieldFromWrongName() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> create().field("i"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Method not found: isI");
    }

    @Test
    void testFromClass() {
        final MergeBuilder<Person> merge = ConflictResolutions.merge(Person.class);
        testMerge(merge);
    }

    @Test
    void testFromClassWithoutDefaultConstructor() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> ConflictResolutions.merge(PersonWithoutDefaultConstructor.class))
                .withCauseInstanceOf(NoSuchMethodException.class)
                .withMessageContaining(PersonWithoutDefaultConstructor.class.getName() + ".<init>()");
    }

    @Test
    void testFromConstructor() {
        final MergeBuilder<Person> merge = ConflictResolutions.merge(Person::new);
        testMerge(merge);
    }

    @Test
    void testNestedFieldFromField() {
        final FieldMergeBuilder<String, Person> nestedField = createWithId()
                .field(FunctionalClass.from(Person.class).field("firstName"));
        testNestedField(nestedField);
    }

    @Test
    void testNestedFieldFromGetterSetter() {
        final FieldMergeBuilder<String, Person> nestedField = createWithId()
                .field(Person::getFirstName, Person::setFirstName);
        testNestedField(nestedField);
    }

    @Test
    void testNestedFieldFromName() {
        final FieldMergeBuilder<String, Person> nestedField = createWithId()
                .field("firstName");
        testNestedField(nestedField);
    }

    @Test
    void testNestedFieldFromWrongField() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> createWithId().field(FunctionalClass.from(Person.class).field("fistName")))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Method not found: isFistName");
    }

    @Test
    void testNestedFieldFromWrongName() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> createWithId().field("fistName"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Method not found: isFistName");
    }

    private enum Gender {
        MALE, FEMALE
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Person {

        String id;
        String firstName;
        String lastName;
        LocalDate birthDate;
        Gender gender;
        // lineage
        String source;
        String originalId;
        LocalDateTime lastModified;
        // fusion information
        @Builder.Default
        Set<String> fusedIds = new HashSet<>();
    }

    private static final class PersonWithoutDefaultConstructor {

        @SuppressWarnings("unused")
        private PersonWithoutDefaultConstructor(final String foo) {

        }
    }

    private static final class PersonWithoutGetter {

        @Setter
        private String id;
    }

    private static final class PersonWithoutSetter {

        @Getter
        private String id;
    }

}
