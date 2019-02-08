/*
 * The MIT License
 *
 * Copyright (c) 2018 bakdata GmbH
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
 *
 */
package com.bakdata.deduplication.person;

import com.bakdata.deduplication.deduplication.HardFusionHandler;
import com.bakdata.deduplication.deduplication.online.OnlineDeduplication;
import com.bakdata.deduplication.deduplication.online.OnlinePairBasedDeduplication;
import com.bakdata.deduplication.duplicate_detection.HardPairHandler;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class PersonDeduplication implements OnlineDeduplication<Person> {
    @Delegate
    OnlineDeduplication<Person> deduplication;

    public PersonDeduplication(final HardPairHandler<Person> hardPairHandler,
                               final HardFusionHandler<Person> hardFusionHandler) {
        this.deduplication = OnlinePairBasedDeduplication.<Person>builder()
            .duplicateDetection(new PersonDuplicateDetection(hardPairHandler))
                .fusion(new PersonFusion())
                .hardFusionHandler(hardFusionHandler)
                .build();
    }
}
