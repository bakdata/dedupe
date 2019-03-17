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
package com.bakdata.util;

import static com.bakdata.util.FunctionalClass.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.beans.IntrospectionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

class FunctionalClassTest {

    @Test
    void testGetConstructor() {
        final Supplier<Person> ctor = of(Person.class).getConstructor();
        assertThat(ctor.get())
                .isNotNull();
    }

    @Test
    void testGetConstructorWithException() {
        final Supplier<PersonWithExceptionConstructor> ctor = of(PersonWithExceptionConstructor.class)
                .getConstructor();
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(ctor::get)
                .withMessage("Foo");
    }

    @Test
    void testGetGetter() {
        final Person person = new Person();
        person.setId("foo");
        final FunctionalProperty<Person, Object> id = of(Person.class).field("id");
        final Function<Person, Object> getter = id.getGetter();
        assertThat(getter.apply(person))
                .isEqualTo("foo");
    }

    @Test
    void testGetGetterForMissingField() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> of(Person.class).field("i"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Unknown property: i")
                .satisfies(
                        exception -> assertThat(exception.getCause()).hasMessageContaining("Method not found: isI"));
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

        final PersonWithExceptionGetter person = new PersonWithExceptionGetter();
        person.setId("foo");
        final FunctionalProperty<PersonWithExceptionGetter, Object> id = of(PersonWithExceptionGetter.class)
                .field("id");
        final Function<PersonWithExceptionGetter, Object> getter = id.getGetter();
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> getter.apply(person))
                .withMessage("Foo");
    }

    @Test
    void testGetMissingConstructor() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> of(PersonWithoutDefaultConstructor.class).getConstructor())
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
                .isThrownBy(() -> of(PersonWithoutGetter.class).field("id"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Unknown property: id")
                .satisfies(
                        exception -> assertThat(exception.getCause()).hasMessageContaining("Method not found: isId"));
    }

    @Test
    void testGetMissingSetter() {

        class PersonWithoutSetter {

            @Getter
            private final String id = null;
        }

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> of(PersonWithoutSetter.class).field("id"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Unknown property: id")
                .satisfies(
                        exception -> assertThat(exception.getCause()).hasMessageContaining("Method not found: setId"));
    }

    @Test
    void testGetSetter() {
        final Person person = new Person();
        final FunctionalProperty<Person, Object> id = of(Person.class).field("id");
        final BiConsumer<Person, Object> setter = id.getSetter();
        setter.accept(person, "foo");
        assertThat(person.getId())
                .isEqualTo("foo");
    }

    @Test
    void testGetSetterForMissingField() {
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> of(Person.class).field("i"))
                .withCauseInstanceOf(IntrospectionException.class)
                .withMessageContaining("Unknown property: i")
                .satisfies(
                        exception -> assertThat(exception.getCause()).hasMessageContaining("Method not found: isI"));
    }

    @Test
    void testGetSetterWithException() {
        @Data
        class PersonWithExceptionSetter {

            private String id;

            public void setId(final String id) {
                throw new UnsupportedOperationException("Foo");
            }
        }

        final PersonWithExceptionSetter person = new PersonWithExceptionSetter();
        final BiConsumer<PersonWithExceptionSetter, Object> setter = of(PersonWithExceptionSetter.class)
                .field("id").getSetter();
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> setter.accept(person, "foo"))
                .withMessage("Foo");
    }

    //cannot inline this class because of NoSuchMethodException for constructor
    static class PersonWithExceptionConstructor {

        PersonWithExceptionConstructor() {
            throw new IllegalStateException("Foo");
        }
    }

    //cannot inline this class because of NoSuchMethodException for constructor
    static class PersonWithoutDefaultConstructor {

        PersonWithoutDefaultConstructor(final Object foo) {

        }
    }

    @Data
    private static class Person {

        private String id;
    }

}
