package com.bakdata.deduplication.fusion;

import static com.bakdata.deduplication.fusion.ConflictResolutions.assumeEqualValue;
import static com.bakdata.deduplication.fusion.ConflictResolutions.latest;
import static com.bakdata.deduplication.fusion.ConflictResolutions.longest;
import static com.bakdata.deduplication.fusion.ConflictResolutions.max;
import static com.bakdata.deduplication.fusion.ConflictResolutions.min;
import static com.bakdata.deduplication.fusion.ConflictResolutions.union;
import static com.bakdata.deduplication.fusion.ConflictResolutions.vote;
import static com.bakdata.util.FunctionalClass.from;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.bakdata.deduplication.fusion.ConflictResolutions.Merge.AdditionalFieldMergeBuilder;
import com.bakdata.deduplication.fusion.ConflictResolutions.Merge.FieldMergeBuilder;
import com.bakdata.deduplication.fusion.ConflictResolutions.Merge.MergeBuilder;
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

    private static Set<String> fusionIdWithPersonId(Person p) {
        return Set.copyOf(Sets.union(p.getFusedIds(), Set.of(p.getId())));
    }

    private static void testField(FieldMergeBuilder<String, Person> field) {
        FieldMergeBuilder<String, Person> nestedField = field.with(min())
            .field(Person::getFirstName, Person::setFirstName);
        testNestedField(nestedField);
    }

    private static void testMerge(MergeBuilder<Person> merge) {
        FieldMergeBuilder<String, Person> field = merge.field(Person::getId, Person::setId);
        testField(field);
    }

    private static void testNestedField(FieldMergeBuilder<String, Person> nestedField) {
        ConflictResolution<Person, Person> resolution = nestedField.with(longest()).then(vote())
            .field(Person::getLastName, Person::setLastName).correspondingToPrevious()
            .field(Person::getGender, Person::setGender).with(assumeEqualValue())
            .field(Person::getBirthDate, Person::setBirthDate).with(vote()).then(latest())
            .field(Person::getLastModified, Person::setLastModified).with(max())
            .field(ConflictResolutionsTest::fusionIdWithPersonId, Person::setFusedIds).with(union())
            .build();
        testResolution(resolution);
    }

    private static void testResolution(ConflictResolution<Person, Person> resolution) {
        assertThat(resolution)
            .isNotNull();
        FusionContext context = new FusionContext();
        Person person1 = Person.builder()
            .id("id1")
            .firstName("Joanna")
            .lastName("Doe")
            .gender(Gender.FEMALE)
            .birthDate(LocalDate.of(2017, Month.DECEMBER, 31))
            .lastModified(LocalDateTime.MAX)
            .build();
        Source source1 = new Source("source1", 1.0f);
        LocalDateTime dateTime = LocalDateTime.MIN;
        Person person2 = Person.builder()
            .id("id2")
            .firstName("John")
            .lastName("Smith")
            .gender(Gender.MALE)
            .birthDate(LocalDate.of(2018, Month.JANUARY, 1))
            .lastModified(LocalDateTime.MIN)
            .build();
        Source source2 = new Source("source2", 2.0f);
        List<AnnotatedValue<Person>> values = List.of(
            new AnnotatedValue<>(person1, source1, dateTime),
            new AnnotatedValue<>(person2, source2, dateTime));
        Optional<Person> resolved = resolution.resolve(values, context);
        Person expected = Person.builder()
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
        List<Exception> exceptions = context.getExceptions();
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
        FieldMergeBuilder<String, Person> field = create()
            .field(FunctionalClass.from(Person.class).field("id"));
        testField(field);
    }

    @Test
    void testFieldFromGetterSetter() {
        FieldMergeBuilder<String, Person> field = create()
            .field(Person::getId, Person::setId);
        testField(field);
    }

    @Test
    void testFieldFromName() {
        FieldMergeBuilder<String, Person> field = create()
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
            .isThrownBy(() -> create().field(from(Person.class).field("i")))
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
        MergeBuilder<Person> merge = ConflictResolutions.merge(Person.class);
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
        MergeBuilder<Person> merge = ConflictResolutions.merge(Person::new);
        testMerge(merge);
    }

    @Test
    void testNestedFieldFromField() {
        FieldMergeBuilder<String, Person> nestedField = createWithId()
            .field(from(Person.class).field("firstName"));
        testNestedField(nestedField);
    }

    @Test
    void testNestedFieldFromGetterSetter() {
        FieldMergeBuilder<String, Person> nestedField = createWithId()
            .field(Person::getFirstName, Person::setFirstName);
        testNestedField(nestedField);
    }

    @Test
    void testNestedFieldFromName() {
        FieldMergeBuilder<String, Person> nestedField = createWithId()
            .field("firstName");
        testNestedField(nestedField);
    }

    @Test
    void testNestedFieldFromWrongField() {
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> createWithId().field(from(Person.class).field("fistName")))
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

        private PersonWithoutDefaultConstructor(String foo) {

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