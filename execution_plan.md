# Execution Plan: SEO Article Generation System

## Phase 1: Rule Deconstruction & Prompt Logic Mapping
**Goal**: Convert the static "SEO Guidelines" (19 rules) into actionable prompt constraints.

*   **Review & Categorize Rules**
    *   **Action**: Analyze the 19 SEO rules and sort them into three categories: *Structural* (headings, length), *Content* (keywords, tone), and *Formatting* (links, lists).
    *   **Why**: distinct rule types require different placement in the prompt (e.g., System Message vs. User User).
    *   **Output**: A `Rule_Mapping_Doc` linking each rule to a specific prompt section.
    *   **Effort**: 1 Day

*   **Draft System Instructions**
    *   **Action**: Create a "Master System Prompt" that encapsulates the non-negotiable SEO rules (e.g., "Always use H2 for main sections").
    *   **Why**: Sets the baseline behavior for the model across all turns.
    *   **Output**: `v1_system_prompt.txt`.
    *   **Effort**: 0.5 Days

*   **Define Constraint Injection Strategy**
    *   **Action**: Determine how strictly to enforce rules (e.g., negative constraints "Do not use..." vs positive "Must include...").
    *   **Why**: LLMs follow positive instructions better; negative constraints need careful phrasing.
    *   **Output**: specific phrasing syntax for hard constraints.
    *   **Effort**: 0.5 Days

## Phase 2: Prompt Architecture & Workflow Design
**Goal**: Design the chain of prompts to ensure high-quality, long-form content.

*   **Select Chaining Strategy**
    *   **Action**: Adopt a **Multi-Step Chain** workflow:
        1.  *Outline Generation* (Title + Keywords -> Outline with H2/H3s)
        2.  *Section Drafting* (Outline Item + Rules -> Full Section Content)
        3.  *Refinement/Assembly* (Combine + Final Polish)
    *   **Why**: Single-shot generation often forgets rules in long outputs (catastrophic forgetting). Chaining ensures focus on each section.
    *   **Output**: Workflow Diagram / Step definitions.
    *   **Effort**: 1 Day

*   **Structure the "Outline" Prompt**
    *   **Action**: Design prompt to accept `Title` + `Keywords` and output a structured JSON outline.
    *   **Why**: Validates the structure *before* writing prose. Saves tokens by catching errors early.
    *   **Output**: `prompt_outline_gen.txt`.
    *   **Effort**: 0.5 Days

*   **Structure the "Section Writer" Prompt**
    *   **Action**: Design prompt to take a specific H2/H3 and its specific keywords/URLs and write *just* that section.
    *   **Why**: Maximizes context window attention on specific keyword density and link placement rules.
    *   **Output**: `prompt_section_writer.txt`.
    *   **Effort**: 1 Day

## Phase 3: Input Injection & API Orchestration (Python)
**Goal**: Build the script to pass user inputs securely into the prompts.

*   **Develop Input Injection Mechanism**
    *   **Action**: Create Python f-strings or Jinja2 templates for dynamic prompt population.
        *   Inject `keywords` list into the System Prompt.
        *   Inject specific `urls` (text/link objects) into the relevant Section Prompts.
    *   **Why**: Hard-coding is impossible; dynamic injection must be robust against special characters.
    *   **Output**: Python functions `format_prompt(template, inputs)`.
    *   **Effort**: 1 Day

*   **Implement OpenRouter API Logic**
    *   **Action**: Write Python wrapper for OpenRouter API calls.
        *   Handle context limits (managing history if using chat endpoint).
        *   Implement retries for API timeouts.
    *   **Why**: Reliable execution layer.
    *   **Output**: `api_client.py` module.
    *   **Effort**: 0.5 Days

*   **URL Placement Logic**
    *   **Action**: Write logic to assign specific URLs to specific sections *before* generation, or instruct the LLM to pick naturally.
    *   **Why**: Ensures the "Link" input objects are actually used in the text.
    *   **Output**: Logic in the orchestration script.
    *   **Effort**: 0.5 Days

## Phase 4: Image Generation Integration
**Goal**: Generate relevant visuals automatically.

*   **Design Image Prompt Generation Step**
    *   **Action**: Add a step *after* Outline Generation: "Generate a DALL-E/Midjourney prompt description based on this article title and outline."
    *   **Why**: SEO articles need relevant alt-text and visuals. The text model is best at describing the image needs.
    *   **Output**: `prompt_image_desc_gen.txt`.
    *   **Effort**: 0.5 Days

*   **Implement Image API Call**
    *   **Action**: Python script triggers the Image Model using the generated description.
    *   **Why**: Asynchronous generation parallel to text writing.
    *   **Output**: Image file saved locally or URL ready for insertion.
    *   **Effort**: 0.5 Days

*   **Integration in Final Output**
    *   **Action**: Insert `![Alt Text](path/to/image)` into the final Markdown.
    *   **Why**: Completes the article format.
    *   **Output**: Final assembly logic.
    *   **Effort**: 0.5 Days

## Phase 5: Validation & Iterative Refinement
**Goal**: Ensure output matches the 19 Rules.

*   **Develop Compliance Check Script**
    *   **Action**: Write simple Python checks for "Hard Rules":
        *   Does it contain the primary keyword?
        *   Are there H2s?
        *   Are the injected URLs present?
    *   **Why**: Instant feedback loop without human reading.
    *   **Output**: `validator.py`.
    *   **Effort**: 1 Day

*   **Prompt Iteration Loop**
    *   **Action**: Run 10 articles. Audit against the "19 Rules".
        *   If Rule X is missed -> Add "Strongly Emphasize Rule X" to System Prompt.
        *   If Tone is off -> Adjust Persona in System Prompt.
    *   **Why**: Prompts are rarely perfect v1.
    *   **Output**: `v2_prompts` files.
    *   **Effort**: 2 Days

*   **Final Acceptance Testing**
    *   **Action**: Blind review of 5 articles.
    *   **Why**: Human quality assurance on "flow" and "readability".
    *   **Output**: Sign-off for production.
    *   **Effort**: 1 Day
