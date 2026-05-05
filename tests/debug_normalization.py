import sys
# Set encoding to utf-8 for stdout
sys.stdout.reconfigure(encoding='utf-8')

from src.services.validation_service import ValidationService

v = ValidationService()
pk = v._normalize_heading_label("شركة تنظيف بالرياض")
h1 = v._normalize_heading_label("شركة تنظيف بالرياض رخيصة")
h2 = v._normalize_heading_label("مميزات أفضل شركة تنظيف بالرياض")
h3 = v._normalize_heading_label("أسعار شركة تنظيف بالرياض")

print(f"pk: '{pk}'")
print(f"h1: '{h1}' -> pk in h1? {pk in h1}")
print(f"h2: '{h2}' -> pk in h2? {pk in h2}")
print(f"h3: '{h3}' -> pk in h3? {pk in h3}")

print("Testing generic heading:")
print(f"الخاتمة: '{v._normalize_heading_label('الخاتمة')}'")
print(f"Is generic: {v._is_generic_visible_heading('الخاتمة')}")
