import sys
sys.stdout.reconfigure(encoding='utf-8')

target_file = 'src/services/brand_evidence_service.py'

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

target_str = """def retrieve_brand_source_chunks(section: dict, state: dict, top_k: int = 3) -> List[Dict[str, Any]]:
    # Get chunks or compile them dynamically
    chunks = state.get("brand_source_chunks")
    if chunks is None:
        chunks = build_brand_source_chunks(state)
        state["brand_source_chunks"] = chunks
        
    if not chunks:
        return []
        
    heading = (section.get("heading_text") or "").lower()
    purpose = (section.get("content_goal") or "").lower()
    intent = (section.get("section_intent") or "").lower()
    assigned_keywords = [k.lower() for k in section.get("assigned_keywords", []) if isinstance(k, str)]
    primary_keyword = (state.get("primary_keyword") or "").lower()
    brand_name = (state.get("brand_name") or "").lower()"""

replacement_str = """def retrieve_brand_source_chunks(section: dict, state: dict, top_k: int = 3) -> List[Dict[str, Any]]:
    # Get chunks or compile them dynamically
    chunks = state.get("brand_source_chunks")
    if chunks is None:
        chunks = build_brand_source_chunks(state)
        state["brand_source_chunks"] = chunks
        
    if not chunks:
        return []
        
    heading = (section.get("heading_text") or "").lower()
    purpose = (section.get("content_goal") or "").lower()
    intent = (section.get("section_intent") or "").lower()
    assigned_keywords = [k.lower() for k in section.get("assigned_keywords", []) if isinstance(k, str)]
    primary_keyword = (state.get("primary_keyword") or "").lower()
    brand_name = (state.get("brand_name") or "").lower()
    
    # Pre-flight relevance guard (Phase 1.7 Step 8)
    brand_aliases = state.get("brand_aliases")
    if not isinstance(brand_aliases, list) or not all(isinstance(a, str) and a.strip() for a in brand_aliases):
        brand_aliases = []
    brand_aliases = [a.lower().strip() for a in brand_aliases if a.strip()]
    brand_refs = [brand_name] + brand_aliases if brand_name else brand_aliases
    
    heading_lower = heading.lower()
    purpose_lower = purpose.lower()
    intent_lower = intent.lower()
    
    references_brand = False
    if brand_refs:
        for ref in brand_refs:
            if ref in heading_lower or ref in purpose_lower or ref in intent_lower or any(ref in k for k in assigned_keywords):
                references_brand = True
                break
                
    is_evidence_intent = intent_lower in ["commercial", "faq", "brand_proof", "brand_evidence"]
    
    if not (references_brand or is_evidence_intent):
        return []"""

if target_str in content:
    content = content.replace(target_str, replacement_str, 1)
    with open(target_file, 'w', encoding='utf-8') as f:
        f.write(content)
    print("SUCCESS: retrieve_brand_source_chunks relevance guard added!")
else:
    print("ERROR: Target not found!")
