import inspect
from src.services.content_generator import OutlineGenerator

gen = OutlineGenerator(None)
sig = inspect.signature(gen.generate)
print(sig)
