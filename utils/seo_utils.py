def enforce_meta_lengths(meta: dict) -> dict:
    """
    Ensures meta title respects SEO length rules.
    Meta description is kept as-is (no truncation).
    """

    title = meta.get("meta_title", "")
    description = meta.get("meta_description", "")

    # Only trim meta_title for SEO compliance
    meta["meta_title"] = title[:70]
    meta["meta_description"] = description  # No truncation

    return meta
