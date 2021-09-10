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
package com.bakdata.dedupe.duplicate_detection.offline;

import com.bakdata.dedupe.duplicate_detection.DuplicateDetection;

/**
 * The offline duplicate detection returns all duplicate records within a dataset.
 * <p>Consider a dataset of records A, B, A' and A, A' being duplicates. The final result will be [(A, A'), (B)].</p>
 * <p>The actual implementation may use any means necessary to find duplicates and to ensure proper transitivity ((A,B)
 * is duplicate and (B,C) is duplicate implies that (A,C) is duplicate).</p>
 *
 * @implSpec For offline algorithms, the general assumption is that they work stateless. Derivations need to be
 * documented.
 */
@FunctionalInterface
public interface OfflineDuplicateDetection<C extends Comparable<C>, T> extends DuplicateDetection<C, T> {
}
