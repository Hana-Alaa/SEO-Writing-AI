import json
import logging
import asyncio
from typing import List, Dict, Any, Optional
from jinja2 import Template, StrictUndefined
from utils.safe_json import recover_json

logger = logging.getLogger(__name__)

class ContentGeneratorError(Exception):
    """Base exception for content generation errors."""
    pass

# class OutlineGenerator:
#     """
#     Handles article outline generation using SEO-optimized prompts (Async).
#     """
#     def __init__(self, ai_client: Any, template_path: str = "prompts/templates/step1_outline_gen.txt"):
#         self.ai_client = ai_client
#         with open(template_path, "r", encoding="utf-8") as f:
#             self.template = Template(f.read(), undefined=StrictUndefined)

#     async def generate(self, title: str, keywords: List[str], urls: List[Dict[str, str]]) -> List[Dict[str, Any]]:
#         """
#         Generates a structured article outline asynchronously, including URLs.
#         """
#         prompt = self.template.render(
#             title=title,
#             keywords=keywords,
#             urls=urls  
#         )
        
#         try:
#             response = await self.ai_client.send(prompt)
#             if not response:
#                 raise ContentGeneratorError("AI returned empty response for outline.")
                
#             clean_response = response.strip().replace("```json", "").replace("```", "").strip()
#             outline = json.loads(clean_response)

#             outline = recover_json(raw_text)

#             if not outline or not isinstance(outline, list):
#                 logger.error("Outline recovery failed.")
#                 logger.debug(f"RAW OUTLINE:\n{raw_text}")
#                 return []

            
#             if not isinstance(outline, list):
#                 raise ContentGeneratorError("AI returned invalid outline format (not a list).")
            
#             # Assign URLs to sections heuristically if not done by AI
#             for section in outline:
#                 section.setdefault("assigned_links", [])
            
#             return outline
            
#         except json.JSONDecodeError as e:
#             logger.error(f"Failed to parse outline JSON: {e}")
#             raise ContentGeneratorError(f"AI returned invalid JSON for outline: {e}")


class OutlineGenerator:
    def __init__(self, ai_client: Any, template_path: str = "prompts/templates/step1_outline_gen.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def generate(
        self,
        title: str,
        keywords: List[str],
        urls: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:

        prompt = self.template.render(
            title=title,
            keywords=keywords,
            urls=urls
        )

        # response = await self.ai_client.send(prompt)
        response = await self.ai_client.send(prompt, step="outline")


        if not response:
            logger.error("Outline AI returned empty response")
            return []

        outline = recover_json(response)

        if not outline or not isinstance(outline, list):
            logger.error("Outline JSON recovery failed")
            logger.debug(f"RAW OUTLINE RESPONSE:\n{response}")
            return []

        # Normalize sections
        for idx, section in enumerate(outline):
            section.setdefault("section_id", f"section_{idx+1}")
            section.setdefault("assigned_links", [])

        return outline


class SectionWriter:
    """
    Handles writing content for specific article sections (Async).
    """
    def __init__(self, ai_client: Any, template_path: str = "prompts/templates/step2_section_writer.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def write(self, title: str, global_keywords: List[str], section: Dict[str, Any]) -> str:
        """
        Writes content for a specific section asynchronously.
        """
        # Ensure section has all required keys for the template
        safe_section = {
            "heading_level": section.get("heading_level", "H2"),
            "heading_text": section.get("heading_text", "Untitled Section"),
            "section_intent": section.get("section_intent", "Write informative content."),
            "assigned_keywords": section.get("assigned_keywords", []),
            "assigned_links": section.get("assigned_links", []) + section.get("urls", []),  
            "estimated_word_count": section.get("estimated_word_count", 300)
        }
        
        prompt = self.template.render(
            title=title,
            global_keywords=global_keywords,
            section=safe_section
        )
        
        try:
            # content = await self.ai_client.send(prompt)
            content = await self.ai_client.send(prompt, step="section")
            if not content:
                logger.warning(f"AI returned empty content for section {section.get('section_id')}")
                return ""
            # return content.strip().replace("```", "").strip()
            return content.strip().removeprefix("```").removesuffix("```").strip()
        except Exception as e:
            logger.error(f"Error writing section {section.get('section_id', 'unknown')}: {e}")
            raise ContentGeneratorError(f"Section writing failed: {e}")

# class Assembler:
#     """
#     Handles assembling sections into a final article (Async).
#     """
#     def __init__(self, ai_client: Any, template_path: str = "prompts/templates/step3_assembly.txt"):
#         self.ai_client = ai_client
#         with open(template_path, "r", encoding="utf-8") as f:
#             self.template = Template(f.read(), undefined=StrictUndefined)

#     async def assemble(self, title: str, sections: List[Dict[str, Any]]) -> Dict[str, str]:
#         """
#         Assembles all sections asynchronously.
#         """
#         prompt = self.template.render(title=title, sections=sections)
        
#         try:
#             response = await self.ai_client.send(prompt)
#             if not response:
#                 raise ContentGeneratorError("AI returned empty response for assembly.")

#             clean_response = response.strip().replace("```json", "").replace("```", "").strip()
#             final_data = json.loads(clean_response)
            
#             required_keys = ["final_markdown", "meta_title", "meta_description"]
#             for key in required_keys:
#                 if key not in final_data:
#                     final_data[key] = "" 


#             from utils.safe_json import recover_json

#             raw_response = await self.ai_client.send(prompt)
#             ai_data = recover_json(raw_response)

#             if not ai_data or not isinstance(ai_data, dict):
#                 logger.error("Assembly JSON recovery failed.")
#                 logger.debug(f"RAW ASSEMBLY RESPONSE:\n{raw_response}")
#                 ai_data = {}

#             final_output = {
#                 "final_markdown": ai_data.get("final_markdown", ""),
#                 "meta_title": ai_data.get("meta_title", ""),
#                 "meta_description": ai_data.get("meta_description", ""),
#                 "raw_text": raw_response
#             }

                    
#             return final_output
            
#         except json.JSONDecodeError as e:
#             logger.error(f"Failed to parse assembler JSON: {e}")
#             raise ContentGeneratorError(f"AI returned invalid JSON for article assembly: {e}")
#         except Exception as e:
#             logger.error(f"Error during article assembly: {e}")
#             raise ContentGeneratorError(f"Article assembly failed: {e}")

# class Assembler:
#     def __init__(self, ai_client: Any, template_path: str = "prompts/templates/step3_assembly.txt"):
#         self.ai_client = ai_client
#         with open(template_path, "r", encoding="utf-8") as f:
#             self.template = Template(f.read(), undefined=StrictUndefined)

#     async def assemble(
#         self,
#         title: str,
#         sections: List[Dict[str, Any]]
#     ) -> Dict[str, str]:

#         prompt = self.template.render(
#             title=title,
#             sections=sections
#         )

#         response = await self.ai_client.send(prompt)

#         if not response:
#             logger.error("Assembly AI returned empty response")
#             return {
#                 "final_markdown": "",
#                 "meta_title": "",
#                 "meta_description": "",
#                 "raw_text": ""
#             }

#         ai_data = recover_json(response)

#         if not ai_data or not isinstance(ai_data, dict):
#             logger.error("Assembly JSON recovery failed")
#             logger.debug(f"RAW ASSEMBLY RESPONSE:\n{response}")
#             ai_data = {}

#         return {
#             "final_markdown": ai_data.get("final_markdown", ""),
#             "meta_title": ai_data.get("meta_title", ""),
#             "meta_description": ai_data.get("meta_description", ""),
#             "raw_text": response
#         }

class Assembler:
    def __init__(self, ai_client: Any, template_path: str = "prompts/templates/step3_assembly.txt"):
        self.ai_client = ai_client
        with open(template_path, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), undefined=StrictUndefined)

    async def assemble(
        self,
        title: str,
        sections: List[Dict[str, Any]],
        image_plan: Optional[List[Dict[str, Any]]] = None  # <-- Pass image prompts here
    ) -> Dict[str, str]:

        # Map section_id -> image details
        image_map = {img['section_id']: img for img in image_plan} if image_plan else {}

        # Insert image placeholders and minimal transitions
        final_sections = []
        for idx, sec in enumerate(sections):
            content = sec.get("generated_content", "")
            
            # Insert image if available
            img_html = ""
            img_data = image_map.get(sec.get("section_id"))
            if img_data:
                img_html = f'\n\n![{img_data["alt_text"]}]({img_data["local_path"]})\n\n'
            
            # Minimal transition (if not last section)
            transition = ""
            if idx < len(sections) - 1:
                transition = "\n\n" + "Continuing to the next section, we explore..." + "\n\n"
            
            final_sections.append(content + img_html + transition)

        final_markdown = "\n".join(final_sections)

        # Generate metadata using AI (optional, or fallback)
        prompt = self.template.render(title=title, sections=sections)
        # response = await self.ai_client.send(prompt)
        response = await self.ai_client.send(prompt, step="assembly")
        ai_data = recover_json(response)

        return {
            "final_markdown": final_markdown,
            "meta_title": ai_data.get("meta_title", title[:70]),
            "meta_description": ai_data.get("meta_description", f"Read our comprehensive guide on {title}"),
            "raw_text": response
        }

