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

import com.bakdata.dedupe.clustering.Cluster;
import java.util.List;
import lombok.NonNull;
import lombok.Value;


/**
 * A fused value with contextual information. If any exception occurred during fusion ({@link #exceptions} is not
 * empty), the value should be considered incomplete.
 * <p>A fusion may fail if there is no obvious representation of a certain piece of information or ultimately requried
 * fields are unset.</p>
 * <p>A {@link IncompleteFusionHandler} may try to find a better solution (possibly with human interaction).</p>
 *
 * @param <T> the type of the record
 */
@Value
public class FusedValue<T> {
    /**
     * The resulting value. May be empty if the fusion completely failed.
     */
    @NonNull
    T value;
    /**
     * The original values.
     */
    @NonNull
    Cluster<?, T> originalValues;
    /**
     * All exceptions that occurred during fusion.
     */
    @NonNull
    List<Exception> exceptions;
}
