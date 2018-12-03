package com.bakdata.util;

import com.bakdata.util.FunctionalClass.Field;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.bakdata.util.FunctionalClass.from;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class FunctionalClassTest {

    @Test
    void testGetConstructor() {
        Supplier<Person> ctor = from(Person.class).getConstructor();
        assertThat(ctor.get())
            .isNotNull();
    }

    @Test
    void testGetConstructorWithException() {
        Supplier<PersonWithExceptionConstructor> ctor = from(PersonWithExceptionConstructor.class)
            .getConstructor();
        assertThatExceptionOfType(IllegalStateException.class)
            .isThrownBy(ctor::get)
            .withMessage("Foo");
    }

    @Test
    void testGetGetter() {
        Person person = new Person();
        person.setId("foo");
        Field<Person, Object> id = from(Person.class).field("id");
        Function<Person, Object> getter = id.getGetter();
        assertThat(getter.apply(person))
            .isEqualTo("foo");
    }

    @Test
    void testGetGetterForMissingField() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> from(Person.class).field("i"))
            .withCauseInstanceOf(IntrospectionException.class)
            .withMessageContaining("Method not found: isI");
    }

    @Test
    void testGetGetterWithException() {
        @Data
        class PersonWithExceptionGetter {

            private String id;

            public String getId() {
                throw new UnsupportedOperationException("Foo");
            }
        }

        PersonWithExceptionGetter person = new PersonWithExceptionGetter();
        person.setId("foo");
        Field<PersonWithExceptionGetter, Object> id = from(PersonWithExceptionGetter.class)
            .field("id");
        Function<PersonWithExceptionGetter, Object> getter = id.getGetter();
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> getter.apply(person))
            .withMessage("Foo");
    }

    @Test
    void testGetMissingConstructor() {
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> from(PersonWithoutDefaultConstructor.class).getConstructor())
            .withCauseInstanceOf(NoSuchMethodException.class)
            .withMessageContaining(PersonWithoutDefaultConstructor.class.getName() + ".<init>()");
    }

    @Test
    void testGetMissingGetter() {

        class PersonWithoutGetter {

            @Setter
            private String id;
        }

        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> from(PersonWithoutGetter.class).field("id"))
            .withCauseInstanceOf(IntrospectionException.class)
            .withMessageContaining("Method not found: isId");
    }

    @Test
    void testGetMissingSetter() {

        class PersonWithoutSetter {

            @Getter
            private String id;
        }

        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> from(PersonWithoutSetter.class).field("id"))
            .withCauseInstanceOf(IntrospectionException.class)
            .withMessageContaining("Method not found: setId");
    }

    @Test
    void testGetSetter() {
        Person person = new Person();
        Field<Person, Object> id = from(Person.class).field("id");
        BiConsumer<Person, Object> setter = id.getSetter();
        setter.accept(person, "foo");
        assertThat(person.getId())
            .isEqualTo("foo");
    }

    @Test
    void testGetSetterForMissingField() {
        assertThatExceptionOfType(RuntimeException.class)
            .isThrownBy(() -> from(Person.class).field("i"))
            .withCauseInstanceOf(IntrospectionException.class)
            //getter method is also looked up in constructor
            .withMessageContaining("Method not found: isI");
    }

    @Test
    void testGetSetterWithException() {
        @Data
        class PersonWithExceptionSetter {

            private String id;

            public void setId(String id) {
                throw new UnsupportedOperationException("Foo");
            }
        }

        PersonWithExceptionSetter person = new PersonWithExceptionSetter();
        BiConsumer<PersonWithExceptionSetter, Object> setter = from(PersonWithExceptionSetter.class)
            .field("id").getSetter();
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> setter.accept(person, "foo"))
            .withMessage("Foo");
    }

    //cannot inline this class because of NoSuchMethodException for constructor
    static class PersonWithExceptionConstructor {

        public PersonWithExceptionConstructor() {
            throw new IllegalStateException("Foo");
        }
    }

    //cannot inline this class because of NoSuchMethodException for constructor
    static class PersonWithoutDefaultConstructor {

        public PersonWithoutDefaultConstructor(Object foo) {

        }
    }

    @Data
    private static class Person {

        private String id;
    }

}