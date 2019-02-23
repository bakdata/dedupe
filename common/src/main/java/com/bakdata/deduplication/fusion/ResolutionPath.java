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

import static com.bakdata.util.ObjectUtils.isNonEmpty;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class ResolutionPath<T, R> implements ConflictResolution<T, R> {
    Function<T, R> extractor;
    ConflictResolution<R, R> resolution;

    @Override
    public List<AnnotatedValue<R>> resolvePartially(final List<AnnotatedValue<T>> annotatedValues,
            final FusionContext context) {
        final List<AnnotatedValue<R>> fieldValues = annotatedValues.stream()
                .map(ar -> ar.withValue(this.extractor.apply(ar.getValue())))
                .filter(ar -> isNonEmpty(ar.getValue()))
                .collect(Collectors.toList());
        return this.resolution.resolvePartially(fieldValues, context);
    }
}
