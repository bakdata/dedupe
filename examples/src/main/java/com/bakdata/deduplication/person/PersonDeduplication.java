package com.bakdata.deduplication.person;

import com.bakdata.deduplication.deduplication.HardFusionHandler;
import com.bakdata.deduplication.deduplication.HardPairHandler;
import com.bakdata.deduplication.deduplication.online.OnlineDeduplication;
import com.bakdata.deduplication.deduplication.online.OnlinePairBasedDeduplication;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class PersonDeduplication implements OnlineDeduplication<Person> {
    public PersonDeduplication(HardPairHandler<Person> hardPairHandler,
                               HardFusionHandler<Person> hardFusionHandler) {
        this.deduplication = OnlinePairBasedDeduplication.<Person>builder()
                .classifier(new PersonClassifier())
                .candidateSelection(new PersonCandidateSelection())
                .clustering(new PersonClustering())
                .fusion(new PersonFusion())
                .hardFusionHandler(hardFusionHandler)
                .hardPairHandler(hardPairHandler)
                .build();
    }

    @Delegate()
    OnlinePairBasedDeduplication<Person> deduplication;
}
