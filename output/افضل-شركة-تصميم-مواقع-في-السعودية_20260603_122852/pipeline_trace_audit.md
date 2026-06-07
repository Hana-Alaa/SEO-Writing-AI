# Pipeline Trace Audit - Latest Run

Output folder: `F:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260603_122852`

## Verdict

Overall status: **needs_revision**.

The latest run is better architecturally than earlier runs because the writer truth path is mostly isolated: brand-owned sections see the knowledge pack, and legacy extracted sources are hidden. The article still should not be treated as final production content because the repair/fulfillment layer diagnoses problems but does not fully fix them.

## File Stats

- Article: 2024 words, 13052 characters.
- Knowledge pack: 2514 words, 18581 characters.
- Workflow `needs_revision` mentions: 2.
- `writer_truth_trace` mentions: 61.
- FAQ leak repair mentions: 1.

## Truth Path Trace

**What worked**

- Brand sections show `knowledge_pack_visible=true`.
- Neutral market sections show `knowledge_pack_visible=false`.
- Legacy writer-facing sources show false in trace: `legacy_section_source_visible`, `legacy_page_briefs_visible`, `legacy_raw_blocks_visible`, and `legacy_understanding_visible`.

This means the latest truth firewall is doing its main job. The remaining failures are not primarily crawler failures; they are fulfillment and repair failures after the pack is visible.

## Knowledge Pack Trace

The knowledge pack contains useful target-relevant project evidence:

- Baddel appears 12 times in the pack and is explicitly tied to Riyadh, Saudi Arabia.
- Billion appears 12 times in the pack and is explicitly tied to Riyadh, Saudi Arabia.
- Aqar Ya Masr appears 16 times and is tied to Egypt.
- Arab Business Academy appears 4 times and is tied to Iraq.

Important caveat: the pack repeatedly says **No explicit pricing/packages** and **No explicit testimonials**. Those words are boundaries, not positive evidence.

## Article Trace Findings

### 1. Intro Gate Still Fails Partially

The first paragraph includes the primary keyword, which is good. But the intro has a duplicated CTA paragraph and does not include the intended light brand bridge paragraph.

Relevant lines in `article_final.md`:

- Line 1: H1 includes unsupported package wording: `????? ????????`.
- Lines 5-9: CTA paragraph is duplicated.

### 2. Unsupported Pricing/Package Signal

The H1 says `????? ????????`, while the pack says no explicit pricing/packages. This is not a brand pricing table, but it still creates a package promise that is not supported by the brand source.

Root cause: title/meta/heading layer is not fully covered by the same brand-claim guard used for sections.

### 3. Project Proof Uses Evidence, But Not The Best Evidence

The proof section mentions:

- Billion: 1 time(s)
- Aqar Ya Masr: 1 time(s)
- Baddel: 0 time(s)
- Arab Business Academy: 0 time(s)

For a Saudi-targeted article, Baddel and Billion are the strongest available examples. The article mentions Billion but misses Baddel, then uses Aqar Ya Masr from Egypt. That means the proof gate is still too lenient: it accepts one relevant project instead of requiring the top target-relevant safe examples when they exist.

Also, the heading says `????? ???????`, but the pack has no explicit testimonials/reviews. That wording should be downgraded to `????? ?? ?????? Creative Minds` or similar.

### 4. Geography Guard Detects But Does Not Fully Repair

The content-stage report flags unsupported brand geography/market presence claims in several sections. This is correct. A project in Riyadh is evidence for that project location, not a general claim that the brand has local Saudi presence or broad Saudi-market expertise.

Root cause: claim removal is diagnostic/soft in some paths. Critical unsupported geography claims should be rewritten or the article should remain `needs_revision` with exact reasons, which happened here.

### 5. Role Planning Still Has Drift

The section titled `??? ????? ??? ????? ????? ??????? ?? ?????????` is traced as `section_job=process`, while its content behaves like evaluation/comparison guidance. Later the article also adds `?????? ????? ??????? ?????? ?? ???? ??????`, so criteria content appears more than once.

Root cause: role resolver prevents exact buyer-question collisions, but it does not yet catch semantic near-duplicates like comparison/evaluation/process all behaving as criteria.

### 6. FAQ Repair Is Incomplete

The log reports `faq_repair_leak_removed`, but the final FAQ still contains planning-like guidance in answer form such as `???? ??????? ??????... ??? ???????... ????... ????...`.

Root cause: the FAQ sanitizer likely removes obvious leaked blocks/preambles, but not sentence-level leaked planning text inside an answer paragraph.

### 7. Table Gate Is Better

The article contains one valid comparison table. It is not malformed and it is safer than the old project tables. This part improved.

Remaining issue: the table is acceptable, but not especially strong from a conversion/decision perspective. This is now a quality refinement, not a structural failure.

## Quality Score

- Crawling / pack availability: **8/10**
- Truth firewall: **8.5/10**
- Article structure: **7/10**
- Commercial persuasiveness: **6/10**
- Section fulfillment: **5.5/10**
- Final article readiness: **6/10**

Final evaluation: **not failed, but not production-ready**. The pipeline is now diagnosing the right problems; the next work should make the repair/gates enforce them more strictly.

## Next Fixes

1. Apply brand-claim guard to H1/meta/title too, especially package/pricing wording.
2. Fix intro repair so it creates exactly: hook + light brand bridge + one CTA, without duplication.
3. Make project proof require the top 2 target-relevant safe project narratives when available.
4. Downgrade `????? ???????` unless explicit testimonials exist.
5. Add semantic near-duplicate role collision detection for criteria/comparison/process overlap.
6. Make FAQ leak sanitizer sentence-level, not only block-level.
