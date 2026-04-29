from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def _extract_json_after(text: str, marker: str, start: int = 0):
    idx = text.find(marker, start)
    if idx < 0:
        return None
    brace = text.find("{", idx)
    if brace < 0:
        return None

    depth = 0
    in_str = False
    esc = False
    for pos, ch in enumerate(text[brace:], brace):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                raw = text[brace : pos + 1]
                try:
                    return json.loads(raw)
                except Exception:
                    return raw
    return None


def _step_output(text: str, step_name: str):
    pos = text.find(f"WORKFLOW STEP: {step_name}")
    if pos < 0:
        return None
    return _extract_json_after(text, "STEP_OUTPUT:", pos)


def _ai_response(text: str, step_name: str):
    pos = text.find(f"==================== STEP: {step_name}")
    if pos < 0:
        return None
    return _extract_json_after(text, "-------------------- RESPONSE --------------------", pos)


def _pretty(obj) -> str:
    if obj is None:
        return "NOT FOUND"
    if isinstance(obj, str):
        return obj
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _visible_outline(outline_state: dict) -> dict:
    return {
        "title": outline_state.get("seo_meta", {}).get("title")
        or outline_state.get("input_data", {}).get("title"),
        "outline": [
            {
                key: section.get(key)
                for key in [
                    "section_id",
                    "heading_text",
                    "heading_level",
                    "section_type",
                    "section_intent",
                    "subheadings",
                ]
            }
            for section in outline_state.get("outline", [])
            if isinstance(section, dict)
        ],
    }


def main() -> None:
    logs = sorted(
        Path("output").glob("*/workflow.log"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if not logs:
        raise SystemExit("No workflow.log files found under output/")

    log_path = logs[0]
    text = log_path.read_text(encoding="utf-8", errors="replace")

    brand_state = _step_output(text, "brand_discovery") or {}
    serp_state = _step_output(text, "serp_analysis") or {}
    strategy_state = _step_output(text, "content_strategy") or {}
    outline_state = _step_output(text, "outline_generation") or {}

    web_raw = _ai_response(text, "web_research")
    serp_raw_ai = _ai_response(text, "serp_analysis")
    strategy_raw_ai = _ai_response(text, "content_strategy")

    outline_raw = None
    last_outline_response = outline_state.get("last_step_response")
    if isinstance(last_outline_response, str):
        try:
            outline_raw = json.loads(last_outline_response)
        except Exception:
            outline_raw = last_outline_response
    if outline_raw is None:
        outline_raw = _visible_outline(outline_state)

    brand_report = {
        "input_title": brand_state.get("input_data", {}).get("title"),
        "primary_keyword": brand_state.get("primary_keyword"),
        "brand_url": brand_state.get("brand_link_used")
        or brand_state.get("input_data", {}).get("urls"),
        "input_urls": brand_state.get("input_data", {}).get("urls"),
        "content_type_at_brand_step": brand_state.get("content_type"),
        "brand_name": brand_state.get("brand_name"),
        "display_brand_name": brand_state.get("display_brand_name"),
        "official_brand_name": brand_state.get("official_brand_name"),
        "domain_brand_name": brand_state.get("domain_brand_name"),
        "brand_aliases": brand_state.get("brand_aliases"),
        "brand_context_raw": brand_state.get("brand_context_raw"),
        "brand_context": brand_state.get("brand_context"),
    }

    serp_final = {}
    if isinstance(serp_state.get("seo_intelligence"), dict):
        serp_final = serp_state["seo_intelligence"].get("market_analysis", {})

    strategy_final = strategy_state.get("content_strategy") or {}
    outline_visible_final = _visible_outline(outline_state)

    notes: list[str] = []
    brand_name = str(brand_report.get("brand_name") or "").lower()
    if brand_name in {
        "web development company",
        "digital agency",
        "website design company",
    }:
        notes.append(
            "Brand discovery اختار وصف خدمة عام كاسم براند: Web Development Company، "
            "رغم أن input_urls فيها text = Creative Minds."
        )

    intent_ai = serp_final.get("intent_analysis") or {}
    primary_keyword = brand_state.get("primary_keyword") or ""
    if (
        intent_ai.get("confirmed_intent") == "informational"
        and "شركة" in primary_keyword
    ):
        notes.append(
            "SERP analysis اعتبر الكلمة informational رغم أنها provider-selection keyword: "
            "افضل + شركة + خدمة."
        )

    market_angle = strategy_final.get("market_angle", "")
    if any(term in market_angle for term in ["unit type", "buying path", "location fit"]):
        notes.append(
            "Strategy market_angle فيه بقايا صياغة عقارية مثل "
            "unit type/location fit/buying path رغم أن الموضوع تصميم مواقع."
        )

    if "Web Development Company" in _pretty(outline_raw):
        notes.append(
            "Outline استخدم اسم البراند الغلط في title و differentiation H2، "
            "وده غالبًا وراثة مباشرة من Brand discovery."
        )

    report: list[str] = []
    report.append("SEO Workflow Layer Audit - Latest workflow.log")
    report.append("=" * 72)
    report.append(f"Generated at: {datetime.now().isoformat(timespec='seconds')}")
    report.append(f"Log file: {log_path}")
    report.append(f"Keyword: {primary_keyword}")
    report.append("Brand URL: https://cems-it.com/")
    report.append(
        "ملاحظة مهمة: هذا التقرير مبني على آخر لوج موجود قبل إصلاحات 29 أبريل "
        "الخاصة بالبراند و SERP intent firewall، لذلك هو يوضح سبب المشاكل في الرن القديم."
    )
    report.append("")

    report.append("EXECUTIVE FINDINGS")
    report.append("-" * 72)
    if notes:
        for idx, note in enumerate(notes, 1):
            report.append(f"{idx}. {note}")
    else:
        report.append("لم يتم اكتشاف مشاكل كبيرة تلقائيًا.")
    report.append("")

    sections = [
        (
            "1) BRAND DISCOVERY OUTPUT",
            "شرح سريع: دي طبقة استخراج/تثبيت البراند. أي غلط هنا بينزل على العنوان والـ differentiation والـ CTA.",
            brand_report,
        ),
        (
            "2) GOOGLE WEB RESEARCH RAW AI RESPONSE",
            "شرح سريع: ده البحث العادي/سيرب خام. هنا الموديل رجع top results والعناوين المرصودة.",
            web_raw,
        ),
        (
            "3) SERP ANALYSIS RAW AI RESPONSE",
            "شرح سريع: دي طبقة تفسير السيرب. هنا ظهر الخلل: confirmed_intent رجع informational و commercial_signal_strength = 0.0.",
            serp_raw_ai,
        ),
        (
            "4) SERP ANALYSIS FINAL STORED STATE",
            "شرح سريع: ده market_analysis بعد تخزينه في state وقت الرن ده.",
            serp_final,
        ),
        (
            "5) CONTENT STRATEGY RAW AI RESPONSE",
            "شرح سريع: ده رد AI قبل normalization/sanitization.",
            strategy_raw_ai,
        ),
        (
            "6) CONTENT STRATEGY FINAL NORMALIZED STATE",
            "شرح سريع: دي الاستراتيجية النهائية التي دخلت للأوتلاين. لاحظ أن النية صارت commercial، لكن market_angle احتفظ ببعض ألفاظ real-estate template.",
            strategy_final,
        ),
        (
            "7) OUTLINE RAW AI RESPONSE",
            "شرح سريع: ده JSON الأوتلاين الذي رجعه الموديل مباشرة.",
            outline_raw,
        ),
        (
            "8) OUTLINE FINAL VISIBLE FIELDS STORED IN STATE",
            "شرح سريع: نفس الأوتلاين كحقول ظاهرة فقط بعد التخزين، بدون حقول السيستم الداخلية لكل سكشن.",
            outline_visible_final,
        ),
    ]

    for title, description, payload in sections:
        report.append(title)
        report.append("-" * 72)
        report.append(description)
        report.append(_pretty(payload))
        report.append("")

    report.append("WHAT THIS SUGGESTS")
    report.append("-" * 72)
    report.append(
        "1. المشكلة الأساسية في هذا الرن بدأت من Brand discovery: اسم البراند اتثبت "
        "كـ Web Development Company بدل Creative Minds/CEMS أو الاسم الصحيح من الموقع."
    )
    report.append(
        "2. SERP research الخام نفسه كان فيه إشارات commercial واضحة مثل CTA وbest company، "
        "لكن SERP analysis فسّرها كـ informational guide."
    )
    report.append(
        "3. Strategy normalization حوّل المقال إلى brand_commercial، لكنه كان لا يزال "
        "محتاج تنظيف من ألفاظ template عقاري داخل market_angle."
    )
    report.append(
        "4. Outline تحسن من ناحية service framing والأسعار، لكنه ورث اسم البراند الغلط، "
        "وهذا يؤكد أن إصلاح البراند upstream أهم من تعديل العناوين يدويًا."
    )
    report.append(
        "5. آخر إصلاحات تمت بعد هذا اللوج: brand canonicalization + SERP intent firewall "
        "+ diagnostic report path. لذلك يلزم rerun جديد للمقارنة."
    )

    out_path = Path("output") / "latest_workflow_layer_audit_20260428_164802.txt"
    out_path.write_text("\n".join(report), encoding="utf-8")
    print(out_path.resolve())
    print(f"bytes {out_path.stat().st_size}")


if __name__ == "__main__":
    main()
