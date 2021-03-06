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

import lombok.Getter;
import lombok.NonNull;
import lombok.Value;


/**
 * The source of a value to be fused.
 */
@Value
public class Source {
    /**
     * A tag to indicate that the respective value has no real source as it has been created during conflict
     * resolution.
     */
    @Getter
    static final @NonNull Source Calculated = new Source("calculated", 1);
    /**
     * The unknown source is used whenever source extraction in {@link Fusion} failed.
     */
    @Getter
    static final @NonNull Source Unknown = new Source("Unknown", 1);
    /**
     * The name of the source (mostly for debugging).
     */
    @NonNull String name;
    /**
     * The weight of the source, mostly used for weighted majority voting.
     */
    double weight;
}
