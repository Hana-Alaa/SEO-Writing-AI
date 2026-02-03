# Constraint Enforcement Strategy

This document defines how we translate "Hard Rules" and "Soft Rules" into prompt instructions to ensure high compliance.

## 1. Constraint Categories

### Hard Constraints (Non-Negotiable)
**Definition**: Rules that cause immediate failure if violated.
**Examples**:
*   "Must include Main Keyword 12-16 times."
*   "Must include at least 7 images."
*   "Must include CTA in paragraph 1."

**Enforcement Strategy**:
1.  **Positive Instruction**: Tell the model WHAT to do, not just what NOT to do.
    *   *Bad*: "Don't write less than 1000 words."
    *   *Good*: "Write at least 1000 words. Expand naturally to 3000-5000 words if the topic allows."
2.  **Repetition**: Place critical constraints in BOTH the System Prompt and the specific User Step for the section.
3.  **XML/Token Anchoring**: Use clear delimiters for structured output.
4.  **Validation Step**: Logic in the chain *must* validate these before accepting the output.

### Soft Constraints (Stylistic/Quality)
**Definition**: subjective rules about tone, flow, or preference.
**Enforcement Strategy**:
1.  **Persona**: Embed in System Prompt (Commercial vs Informational).
2.  **Adjectives**: "Persuasive", "Action-Oriented", "Authoritative".

## 2. Pre-Processing Logic (Competitor Analysis)
**Rule 19 Integration**:
Before any generation starts, the system (or a specific prompt step) must "Analyze top-ranking pages".
*   **Prompt Strategy**: "Simulate an analysis of the top 3 competitors for '{keyword}'. Identify their H2 structure and missing gaps. Use this gap analysis to build the Outline."

## 3. Global Negative Constraints
**Format**:
*   Group under `### NEGATIVE CONSTRAINTS` in System Prompt.
*   **NEVER** link to competitors.
*   **NEVER** use generic fluff.
*   **NEVER** hallucinate URLs.

## 4. Dynamic Injection
**Do NOT** hardcode variable rules in System Prompt.
**Format for User Message**:
```text
CONTEXT INDEPENDENT VARIABLES:
- TARGET KEYWORD: "{keyword}"
- REQUIRED URLS: {url_list}
- INTENT: {intent} (Commercial/Informational)

INSTRUCTIONS:
1. Write the Introduction.
2. Include CTA for "{offer}" in Para 1.
3. Link key terms to the URLs provided above.
```
