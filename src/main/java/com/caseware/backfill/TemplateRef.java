package com.caseware.backfill;

import java.util.Objects;

/** The template version an engagement was created from. Both parts are always present. */
public record TemplateRef(String templateId, String versionId) {
    public TemplateRef {
        Objects.requireNonNull(templateId, "templateId");
        Objects.requireNonNull(versionId, "versionId");
    }
}
