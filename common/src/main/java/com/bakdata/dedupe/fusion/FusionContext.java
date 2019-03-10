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
package com.bakdata.dedupe.fusion;

import com.bakdata.util.ExceptionContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Delegate;

/**
 * A fusion context captures exceptions in an {@link ExceptionContext} and provides additional configurations that
 * represent cross-cutting concerns, such as null handling.
 */
@SuppressWarnings("WeakerAccess")
@Value
@Builder
public class FusionContext {
    @Delegate
    ExceptionContext exceptionContext = new ExceptionContext();
    Map<ResolutionTag<?>, List<? extends AnnotatedValue<?>>> storedValues = new HashMap<>();

    public <T> void storeValues(final ResolutionTag<T> resolutionTag, final List<AnnotatedValue<T>> annotatedValues) {
        this.storedValues.put(resolutionTag, annotatedValues);
    }

    public boolean isNonEmpty(final Object value) {
        return value != null && !"".equals(value);
    }

    @SuppressWarnings("unchecked")
    public <T> List<AnnotatedValue<T>> retrieveValues(final ResolutionTag<T> resolutionTag) {
        return (List<AnnotatedValue<T>>) this.storedValues.computeIfAbsent(resolutionTag,
                k -> {
                    throw new FusionException("Tried to retrieve " + resolutionTag + " without being stored");
                });
    }
}
