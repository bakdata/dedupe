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

import lombok.NonNull;
import lombok.Value;


/**
 * A resolution tag is a way to refer to a previously, resolved value in the current {@link FusionContext}.
 * <p>Such references are tremendously valuable if some values need to be kept consistent. For instance, if an address
 * needs to be resolved, we can define a complex resolution for city, but we should use the corresponding zip code.</p>
 * <p>The resolution tag doubles as a type tag that allows type-safe access to {@link
 * FusionContext#retrieveValues(ResolutionTag)}</p>.
 *
 * @param <T> the type of the record. Used to allow type-safe access.
 */
@Value
@SuppressWarnings({"unused", "squid:S2326"})
public class ResolutionTag<T> {
    /**
     * The name of the tag for debugging.
     */
    @NonNull String name;
}
