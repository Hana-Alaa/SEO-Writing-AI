import os

PATH = r"f:\SEO-Writing-AI\src\services\content_generator.py"

with open(PATH, 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the broken Arabic characters caused by previous script execution
content = content.replace('["price", "cost", "pricing", "OO3O1O O", "OUU,U?Oc", "O3O1O"]', '["price", "cost", "pricing", "أسعار", "تكلفة", "سعر"]')
content = content.replace('["location", "area", "neighborhood", "OO-USO O", "U.U^U,O1"]', '["location", "area", "neighborhood", "أحياء", "موقع"]')
content = content.replace('"OrOU^O O" in heading', '"خطوات" in heading')
content = content.replace('["comparison", "vs", "U.U,O OU+Oc"]', '["comparison", "vs", "مقارنة"]')
content = content.replace('["proof" or "trust" in taxonomy or "O_U,USU," in heading]', '["proof" or "trust" in taxonomy or "دليل" in heading or "مصداقية" in heading]')

# Wait, the last one was a bit different in logic.
# Original: elif sec_type == "proof" or "trust" in taxonomy or "O_U,USU," in heading:
content = content.replace('elif sec_type == "proof" or "trust" in taxonomy or "O_U,USU," in heading:', 
                          'elif sec_type == "proof" or "trust" in taxonomy or "دليل" in heading or "مصداقية" in heading:')

with open(PATH, 'w', encoding='utf-8') as f:
    f.write(content)

print("Success: Fixed Arabic characters in content_generator.py")
