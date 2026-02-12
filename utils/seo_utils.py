def enforce_meta_lengths(meta: dict) -> dict:
    """
    Ensures meta title and description respect SEO length rules.
    """

    title = meta.get("meta_title", "")
    description = meta.get("meta_description", "")

    # Trim safely
    meta["meta_title"] = title[:70]
    meta["meta_description"] = description[:160]

    return meta
