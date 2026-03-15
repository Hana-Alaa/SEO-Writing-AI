# SEO Writing AI - System Input Specifications (2026)

This document outlines all input parameters available in the SEO Writing AI system, categorized by the operating mode: **Basic Plan (Core)** and **Advanced Customization**.

---

## 1. Basic Plan (Core Mode - Autopilot)
The system operates autonomously to determine strategy, tone, and depth based on minimal user input.

### Required Core Inputs
*   **Topic / Title**: The main subject or target title for the article.
*   **Primary Keyword**: The main SEO term to optimize for.
*   **Target Location (area)**: Geographic target for Local SEO (e.g., "Riyadh", "London").
*   **Internal URLs / Brand URL**: User-provided links from the brand's website. The system prioritizes these for internal linking.

### Optional Core Inputs
*   **Brand Logo (Image)**: Specifically used for final HTML rendering and branding.
*   **Reference Brand URL**: A website URL used to ground the AI's knowledge in specific brand context.

### Automated Logic
In Core Mode, the system automatically handles:
*   Keyword intent detection (Commercial, Informational, etc.).
*   Competitor content analysis from Top 5 SERP results.
*   Automatic tone and article type selection.
*   Default word count (~1000 - 1500 words).
*   Generation of 7 contextually relevant images.
*   Automatic Meta Title & Description generation.
*   **Link Rule**: External links are **DISABLED** by default in Core Mode.

---

## 2. Advanced Customization Inputs
Provides granular control over the writing process and visual output.

### Writing Controls
*   **Language**: Selectable (Arabic, English, etc.).
*   **Tone**: 
    *   Professional
    *   Persuasive
    *   Casual
    *   Technical
*   **Article Type**: 
    *   Informational (Blog/Guide)
    *   Commercial (Service/Sales)
    *   Comparison (Product/Service vs. Product/Service)
*   **Point of View (POV)**:
    *   First person singular (I, me, my)
    *   First person plural (We, us, our)
    *   Second person (You, your)
    *   Third person (He, she, it, they)
*   **Article Size**: 
    *   1000 words
    *   2000 words
    *   3000+ words
*   **Brand Voice**:
    *   **Text Description**: Paste specific style qualities.
    *   **Upload Guidelines (File)**: PDF/Doc instructions for the AI.
    *   **Upload Examples (File)**: Samples of previous high-quality articles.

### Content Structure Controls
*   **Conclusion**: Toggle to include/exclude.
*   **FAQ Section**: Toggle to include/exclude.
*   **Comparison Blocks**: Toggle for specialized comparison tables/text.
*   **Tables & Bullet Lists**: Toggle to enable/disable automated formatting.
*   **Bold Key Terms**: Toggle to highlight keywords and industry USPs.

### Media Control
*   **Number of Images**: Adjustable count (e.g., 1 to 15).
*   **Image Type**: 
    *   Illustration
    *   Infographic
    *   Mockup
    *   Mixed (System Choice)
*   **Image Size (Aspect Ratio)**:
    *   Square (1024x1024)
    *   Wide (1792x1024)
    *   Portrait (1024x1792)
*   **Featured Image**: Toggle to include a primary header image.
*   **Custom Branding Frame**: Toggle to apply a visual border/frame to all generated images.
*   **Logo & Style Reference**: Integrated uploads for brand assets.

### SEO & URL Controls
*   **Custom Keyword Density**: Define specific % (e.g., 1.5%).
*   **Secondary Keywords**: Manual list of semantic terms to integrate.
*   **Competitor Count**: Analyze Top 3, 5, or 10 SERP results.
*   **Manual External Links**: Ability to add specific credible sources (Wikipedia, News, etc.) manually.

---

## Technical Mapping (API)
The system uses `workflow_mode` to distinguish between `core` and `advanced`. When `advanced` is selected, all manual overrides are prioritized over the automated strategy.
