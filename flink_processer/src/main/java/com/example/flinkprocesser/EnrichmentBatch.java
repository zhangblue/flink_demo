package com.example.flinkprocesser;

import java.util.List;

public record EnrichmentBatch(List<OutputEvent> success, List<OutputEvent> errors) {
    public EnrichmentBatch {
        success = List.copyOf(success);
        errors = List.copyOf(errors);
    }
}
