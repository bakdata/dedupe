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
package com.bakdata.dedupe.classifier;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * The classification of a {@link com.bakdata.dedupe.candidate_selection.Candidate} with additional information.
 */
@Value
@Builder
public class ClassificationResult {
    /**
     * The classification.
     */
    @NonNull
    Classification classification;
    /**
     * Some confidence value that depends on the {@link Classifier} implementation. A confidence allows to distinguish
     * sure from unsure classifications.
     * <p>The general recommendation is to have confidence in [0; 1], but it should not be confused with a proper
     * mathematical probability.</p>
     */
    double confidence;
    /**
     * Additional explanation for humans, such as the similarity and threshold (0.933 >= 0.9) or the name of a rule that
     * triggered.
     */
    @Builder.Default
    @NonNull
    String explanation = "";
}
