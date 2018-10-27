package com.bakdata.deduplication.person;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class Person {
    String id;
    String firstName;
    String lastName;
    LocalDate birthDate;
    Gender gender;
    // lineage
    String source;
    String originalId;
    LocalDateTime lastModified;
    // fusion information
    @Builder.Default
    Set<String> fusedIds = new HashSet<>();
}

