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
<<<<<<< HEAD:common/src/main/java/com/bakdata/dedupe/fusion/ResolutionTag.java
package com.bakdata.dedupe.fusion;

import lombok.Value;

@Value
@SuppressWarnings("squid:S2326")
public class ResolutionTag<T> {
    String name;
=======

package com.bakdata.util;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
public final class FunctionalField<R, F> {

    @NonNull
    PropertyDescriptor descriptor;

    public Function<R, F> getGetter() {
        final Method getter = this.descriptor.getReadMethod();
        return new FunctionalMethod<>(getter)::invoke;
    }

    public BiConsumer<R, F> getSetter() {
        final Method setter = this.descriptor.getWriteMethod();
        return new FunctionalMethod<>(setter)::invoke;
    }

>>>>>>> Adding javadoc to common:common/src/main/java/com/bakdata/util/FunctionalField.java
}
