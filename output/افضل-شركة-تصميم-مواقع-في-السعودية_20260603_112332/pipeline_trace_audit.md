# Pipeline Trace Audit

Generated: 2026-06-03 11:58:14

## Run

- Output folder: `F:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260603_112332`
- Article: `F:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260603_112332\article_final.md`
- Knowledge pack: `F:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260603_112332\brand_page_knowledge_pack.md`
- Workflow log: `F:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260603_112332\workflow.log`
- Existing HTML review file: `F:\SEO-Writing-AI\output\افضل-شركة-تصميم-مواقع-في-السعودية_20260603_112332\article_final_styled_review.html`

## Executive Verdict

This run is not a total pipeline failure. The crawl/knowledge-pack side contains useful brand facts, and the article is more structured than earlier outputs. The remaining failures are mainly in **truth-path enforcement, section-role fulfillment, project proof selection, FAQ repair, and semantic quality gates**.

The most important finding: the knowledge pack contains target-relevant project evidence, but the final project/proof section does not reliably use it. That means the problem is not only crawling. It is the handoff from brand knowledge to section writing and post-write validation.

## Brand Crawl / Knowledge Pack Findings

- Knowledge pack available: yes
- Article available: yes
- Log available: yes
- Table blocks in article: 1
- Malformed table starts with separator row: True
- Primary keyword in first paragraph: False
- Spacing issue `??????????`: False

### Important Brand Facts Present In Knowledge Pack

| Fact | Present in pack |
|---|---:|
| Baddel | True |
| Billion | True |
| Aqar Ya Masr | True |
| Arab Business Academy | True |
| No explicit pricing/packages boundary | True |
| No general brand geography boundary | True |

### Important Terms Used In Article

| Term | Count in article |
|---|---:|
| Baddel | 1 |
| Billion | 1 |
| Aqar Ya Masr | 3 |
| Arab Business Academy | 1 |
| Saudi/Riyadh mentions | 0 |

## Log Signals

| Signal | Count |
|---|---:|
| `brand_page_knowledge_pack_context` | 15 |
| `brand_writing_brief_context` | 15 |
| `content_stage_quality_report` | 1 |
| `needs_revision` | 0 |
| `unsupported` | 142 |
| `fulfillment_status` | 57 |

Interpretation:
- `brand_page_knowledge_pack_context` being present is good.
- `brand_writing_brief_context` still appearing in logs is a risk signal. It may be diagnostic-only, but the audit should verify it is not writer-facing or render-facing truth.
- `content_stage_quality_report` passing while semantic issues remain means the gate currently catches formatting more than section fulfillment.

## Section Trace

| Section | Heading | Severity | What Went Wrong | Likely Source | Next Fix |
|---|---|---|---|---|---|
| `sec_01` | افضل شركة تصميم مواقع في السعودية: الخدمات والحلول المتوفرة | Medium | Intro improved structurally, but still broad. It can overload the hook when it mentions too many brand projects early. | Intro gate enforces shape more than reader tension and brand-light restraint. | Keep 3 paragraphs; enforce reader problem + keyword first, light brand bridge second, CTA third. |
| `sec_02` | تصميم مواقع الشركات والمؤسسات | High | Heading promises services/solutions, but body drifts into evaluation criteria language. | Role drift after outline; services role is not protected from criteria phrasing. | Role fulfillment gate: services sections explain offer scope; forbid compare/check/ask language outside criteria. |
| `sec_03` | ما الذي يميز Creative Minds عن شركات تصميم المواقع الأخرى؟ | Medium | Feature section is cleaner but still partly generic and can blend features with local market claims. | Evidence density and geography claim guard are too soft. | Anchor brand features to pack facts or write as neutral market guidance. |
| `sec_04` | نماذج من مشاريع Creative Minds وتجارب العملاء | Medium | Differentiator section is weak/generic and does not use strongest target-relevant proof. | Brand proof selection favors convenient evidence rather than relevance. | Use specific capabilities plus one proof example ranked by target relevance. |
| `sec_05` | مقارنة بين حلول تصميم المواقع: أيها الأنسب لاحتياجاتك؟ | Critical | Proof/projects section misses stronger Riyadh/Saudi project evidence and uses weaker/out-of-area examples. Heading says client experiences without testimonial evidence. | Project proof fulfillment does not enforce target-area ranking for prose, and testimonial wording is not blocked. | Project proof formatter: narrative cards/lists by default, target-area ranking, merge variants, ban client-experience wording without testimonials. |
| `sec_06` | تصميم موقع مخصص أم قوالب جاهزة | Low | Comparison table is structurally valid, but usefulness depends on whether the options are genuinely different. | Table gate checks count/format more than decision usefulness. | Add usefulness rubric for comparison tables. |
| `sec_07` | معايير عملية لاختيار المزود أو الحل الأنسب | Medium | Process section exists but may include standard process assumptions not clearly backed by brand pack. | Process role permits generic project-management assumptions. | Separate observed process facts from safe generic process guidance. |
| `sec_auto_evaluation_criteria` | التكلفة والقيمة المتوقعة قبل اتخاذ القرار | High | Evaluation criteria overlaps with earlier services section because the services body already used criteria language. | Body-level role collision is not detected by current resolver. | Post-write role classifier by body language; rewrite or merge overlapping sections. |
| `sec_auto_cost_value` | أسئلة شائعة حول تصميم المواقع في السعودية | Low | Cost/value is acceptable as market guidance if it avoids brand pricing claims. | Mostly healthy; monitor for invented brand pricing/packages. | Keep market-only policy unless pack contains explicit pricing. |
| `sec_08` | هل يمكن تعديل الموقع بعد التسليم؟ | High | FAQ contains duplicated objection guidance/raw lines and awkward phrasing. | FAQ repair/gate leaks planning guidance into final content. | FAQ builder should output only H3 questions and answers, with de-duplicated answer seeds. |
| `sec_09` | Introduction / no explicit heading | Medium | CTA is safe but not very conversion-strong. | CTA gate checks presence more than persuasion. | CTA contract: summarize value, reduce friction, direct action with brand URL. |

## Root Causes

1. **Semantic gates are weaker than formatting gates.** The log can mark content-stage quality as pass because a table exists and formatting is valid, while services still answer criteria, project proof misses best evidence, and FAQ contains duplicated repair text.

2. **Project proof fulfillment is incomplete.** The knowledge pack can include good project pages, but the writer/proof section may still choose weaker or less relevant projects. The fix is not ?always use a table?; project proof should default to narrative cards/lists and only use a table when records are safe and useful.

3. **Body-level role drift is not stopped.** Headings may be assigned roles correctly, but the body can drift. Example: a services heading can become a criteria section. This needs post-write role fulfillment by body language and evidence usage, not heading labels only.

4. **Geography is still too loose.** A project located in a target area is valid as project proof, but it must not become a general claim that the brand understands or serves the whole local market unless the pack explicitly says so.

5. **FAQ repair is leaking guidance into content.** The FAQ section needs a dedicated clean renderer/generator that outputs only H3 Q&A, with no raw planning/guidance lines.

6. **Old truth-path artifacts still need auditing.** `brand_writing_brief_context`, `observed_*`, `routing_signals.projects`, and legacy project records should remain diagnostics/routing only. Any writer-facing or render-facing path should use the knowledge pack or safe records derived from it.

## Recommended Repair Order

1. **Trace instrumentation first.** Log, per section: section job, brand policy, pack pages passed, selected project proof candidates, writer prompt truth source, table target, and fulfillment result. This prevents guessing.

2. **Project proof path.** Build one safe project-proof formatter from knowledge-pack narratives. Default output: narrative cards/list. Table only if 2+ safe records with comparable fields exist. Enforce target-area ranking and merge web/mobile variants.

3. **Role fulfillment gate.** After writing, classify body behavior. Services must explain included services. Criteria sections own ?compare/check/ask? language. Proof sections own projects. Comparison sections must compare real alternatives.

4. **FAQ clean rebuild.** Force FAQ to H3 Q&A only. Remove duplicated objection seed lines.

5. **Geography and testimonial heading guard.** Block headings like ?client experiences? unless testimonials exist. Block ?projects in [area]? unless selected project proof supports that exact area.

6. **Content-stage status discipline.** If any critical semantic gate fails and safe repair cannot fix it, output `needs_revision` with concrete reasons such as `project_proof_missed_target_relevant_evidence`, `role_drift_services_to_criteria`, `faq_repair_leak`, or `unsupported_local_brand_claim`.

## Bottom Line

The current direction is still viable, but the next step should not be another broad prompt improvement. The next step should be a narrow **truth and fulfillment audit/fix**: prove what each section saw, prove what it used, and block the sections that do not fulfill their assigned role.
