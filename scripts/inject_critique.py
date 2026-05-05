import sys

new_methods = """
    async def critique_outline(
        self,
        primary_keyword: str,
        title: str,
        outline: list,
        content_type: str,
        intent: str,
        area: str,
        entity_phrase: str,
        service_phrase: str,
        display_brand_name: str,
        content_strategy: dict,
        heading_quality_audit: dict
    ) -> dict:
        \"\"\"AI-driven holistic critique of the outline. Diagnostic only.\"\"\"
        try:
            template = self.env.get_template('01c_outline_critique.txt')
            prompt = template.render(
                primary_keyword=primary_keyword,
                title=title,
                outline=outline,
                content_type=content_type,
                intent=intent,
                area=area,
                entity_phrase=entity_phrase,
                service_phrase=service_phrase,
                display_brand_name=display_brand_name,
                content_strategy=content_strategy,
                heading_quality_audit=heading_quality_audit
            )
            
            res = await self.ai_client.send(prompt, step='outline_critique')
            raw = res['content']
            
            if not raw:
                return self._safe_critique_fallback('Empty response from AI critique.')
                
            json_text = self._extract_first_json_object(raw)
            from src.utils.json_utils import recover_json
            data = recover_json(json_text)
            
            if not isinstance(data, dict):
                return self._safe_critique_fallback('Invalid JSON structure in AI critique.')
                
            return data
            
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f'AI Outline Critique failed: {e}')
            return self._safe_critique_fallback(str(e))

    def _safe_critique_fallback(self, error_msg: str) -> dict:
        return {
            'mode': 'critique_only',
            'overall_score': 0,
            'passed': True,
            'summary': f'Critique unavailable due to error: {error_msg}',
            'missing_sections': [],
            'redundant_sections': [],
            'weak_sections': [],
            'h3_issues': [],
            'repetition_issues': [],
            'seo_coverage_gaps': [],
            'brand_alignment_issues': [],
            'faq_issues': [],
            'accepted_variations': [],
            'top_recommendations': []
        }

    def _extract_first_json_object(self, text: str) -> str:
        if not text: return ''
        start = text.find('{')
        if start == -1: return text
        count = 0
        for i in range(start, len(text)):
            if text[i] == '{': count += 1
            elif text[i] == '}':
                count -= 1
                if count == 0: return text[start:i+1]
        return text[start:]
"""

path = 'f:/SEO-Writing-AI/src/services/content_generator.py'
content = open(path, encoding='utf-8').read()
target = 'class SectionWriter:'
if target in content:
    parts = content.split(target)
    # Re-insert with the new methods before SectionWriter
    new_content = parts[0] + new_methods + '\n' + target + parts[1]
    with open(path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print('SUCCESS')
else:
    print('TARGET NOT FOUND')
