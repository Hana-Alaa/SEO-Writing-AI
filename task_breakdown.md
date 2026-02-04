# Task Breakdown: SEO Article Generation System

## Phase 1: Rule Deconstruction & Prompt Logic Mapping

### Task 1.1: SEO Rule Categorization & Mapping
- **Purpose**: To convert the flat list of 19 SEO rules into structured constraints that can be placed in the correct part of the prompt chain.
- **Input**: `rules/seo_rules.md` (The 19 Rules).
- **Output**: `rules/rule_mapping.md` (Table mapping Rule ID -> Prompt Section [System/User/Outline/Body] -> Constraint Type [Hard/Soft]).
- **Dependencies**: None.

### Task 1.2: Master System Prompt Drafting
- **Purpose**: To establish the base "Expert SEO Writer" persona and generally applicable negative constraints that apply to *all* generation steps.
- **Input**: Rule Mapping (from Task 1.1).
- **Output**: `prompts/system/master_system_prompt.txt`.
- **Dependencies**: Task 1.1.

### Task 1.3: Constraint Enforcement Strategy
- **Purpose**: To decide how to phrase "Hard Rules" so the LLM cannot ignore them (e.g., XML tags vs Markdown instructions).
- **Input**: LLM Best Practices for the target model.
- **Output**: `docs/constraint_strategy.md` (Guidelines for positive vs negative instruction phrasing).
- **Dependencies**: None.

## Phase 2: Prompt Architecture & Workflow Design (Chained)

### Task 2.1: Workflow Schema Design
- **Purpose**: To define the data structure passed between steps in the chain to prevent data loss.
- **Input**: Project Requirements (Title, Keywords, URLs).
- **Output**: `docs/workflow_schema.json` (Defining the JSON structure for the Outline and Section intermediate states).
- **Dependencies**: None.

### Task 2.2: "Outline Generation" Prompt Design
- **Purpose**: To generate a compliant H2/H3 structure *before* writing any text.
- **Input**: User `Title` + `Keywords` list.
- **Output**: `prompts/templates/step1_outline_gen.txt` (Jinja2 template).
- **Dependencies**: Task 1.2 (System Prompt).

### Task 2.3: "Section Writer" Prompt Design
- **Purpose**: To write a single section focused on specific keywords and rules, avoiding context overload.
- **Input**: Single Outline Node (H2), Global Keyword List, Assigned URLs for this section.
- **Output**: `prompts/templates/step2_section_writer.txt`.
- **Dependencies**: Task 2.1.

### Task 2.4: "Final Assembly" Prompt Design
- **Purpose**: To stitch sections together and ensure transitions/flow without breaking formatting.
- **Input**: List of generated Markdown sections.
- **Output**: `prompts/templates/step3_assembly.txt`.
- **Dependencies**: Task 2.3.

## Phase 3: Input Injection & API Orchestration

### Task 3.1: Input Validation Schema
- **Purpose**: To ensure user inputs match the strict requirement (Title: str, Keywords: list, URLs: list[obj]).
- **Input**: Raw User JSON.
- **Output**: `schemas/input_validator.py` (Pydantic models).
- **Dependencies**: None.

### Task 3.2: Dynamic Keyword & URL Injection Logic
- **Purpose**: To programmatically insert the right keywords into the right prompt steps.
- **Input**: User Inputs.
- **Output**: `utils/injector.py` (Functions: `distribute_urls_to_outline()`, `format_prompt_variables()`).
- **Dependencies**: Task 2.1.

### Task 3.3: OpenRouter Client Implementation
- **Purpose**: To handle the actual API communication, including retries and model selection.
- **Input**: OpenRouter API Key.
- **Output**: `services/openrouter_client.py` (Class with `generate_completion` method handling retries).
- **Dependencies**: None.

## Phase 4: Image Generation Integration

### Task 4.1: Image Prompt Generation
- **Purpose**: To convert the article topic into a visual description suitable for an image model.
- **Input**: Article Title + Generated Outline.
- **Output**: `prompts/templates/image_prompt_gen.txt`.
- **Dependencies**: Task 2.2 (Outline).

### Task 4.2: Image API Integration
- **Purpose**: To fetch the image and return a valid URL/Path.
- **Input**: Image Description.
- **Output**: `services/image_generator.py`.
- **Dependencies**: Task 4.1.

## Phase 5: Orchestration Layer 
Tasks في Orchestration
Task 4.5.1: Workflow Controller

Purpose: إدارة ترتيب الخطوات من Input → Outline → Sections → Assembly → Image Generation.

Input: ArticleInput object + optional URLs.

Output: workflow_state.json أو in-memory dict.

Dependencies: Phase 1–4.

Task 4.5.2: Step-by-Step Executor

Purpose: تنفيذ كل step في sequence والتحقق من نجاحه قبل الانتقال للـ next step.

Steps:

Outline Generation → Step 1

Section Writing → Step 2

Assembly → Step 3

Image Generation → Step 4

Output: Updated workflow_state with all generated content.

Dependencies: Task 4.5.1

Task 4.5.3: Image Download & Responsive Versions

Purpose: بعد ما الـ image URLs اتولدت، نحمل الصور وننشئ نسخ responsive (Featured, Inline, Thumbnail).

Input: Image prompts list من Step 4.

Output: Local image files + paths في workflow_state.

Dependencies: Task 4.5.2

Task 4.5.4: Final Output Assembly

Purpose: إنشاء الكائن النهائي اللي هيدخل للـ validator:

{
  "final_markdown": "...",
  "meta_title": "...",
  "meta_description": "...",
  "images": [...],
  "workflow_state": {...}
}


Input: Workflow state بعد كل الخطوات.

Output: JSON / dict جاهز للـ Phase 5.

Dependencies: Task 4.5.2, Task 4.5.3

## Phase 6: Validation & Iteration

### Task 6.1: Automated Compliance Validator
- **Purpose**: To run code-based checks on the output to verify "Hard Rules" (Keywords present? H2s exist? Links valid?).
- **Input**: Final Generated Markdown.
- **Output**: `utils/seo_validator.py` (Functions returning Pass/Fail).
- **Dependencies**: Task 1.1 (Rule Rules).

### Task 6.2: Iteration & Training Loop
- **Purpose**: To systematically improve prompts based on validator failures.
- **Input**: Validator Reports.
- **Output**: `docs/iteration_log.md` and updated prompt templates (v1.1, v1.2).
- **Dependencies**: Task 5.1.
