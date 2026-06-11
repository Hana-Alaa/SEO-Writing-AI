"""Project-root path helpers for assets and prompt templates."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROMPT_TEMPLATES_DIR = PROJECT_ROOT / "assets" / "prompts" / "templates"


class PromptTemplatesNotFoundError(FileNotFoundError):
    """Raised when the prompt templates directory or a template file is missing."""


def require_prompt_templates_dir() -> Path:
    resolved = PROMPT_TEMPLATES_DIR.resolve()
    if not PROMPT_TEMPLATES_DIR.is_dir():
        raise PromptTemplatesNotFoundError(
            f"Prompt templates directory not found: {resolved}"
        )
    return PROMPT_TEMPLATES_DIR


def prompt_template_path(filename: str) -> Path:
    require_prompt_templates_dir()
    path = PROMPT_TEMPLATES_DIR / filename
    if not path.is_file():
        raise PromptTemplatesNotFoundError(
            f"Prompt template not found: {path.resolve()}"
        )
    return path


def resolve_prompt_template_path(path_or_name: str | Path) -> Path:
    """Resolve a template filename or legacy project-relative path."""
    candidate = Path(path_or_name)
    if candidate.is_file():
        return candidate.resolve()

    if not candidate.is_absolute():
        legacy = PROJECT_ROOT / candidate
        if legacy.is_file():
            return legacy.resolve()

    return prompt_template_path(candidate.name)


def create_prompt_template_loader():
    from jinja2 import FileSystemLoader

    return FileSystemLoader(str(require_prompt_templates_dir()))


def read_prompt_template(filename: str, encoding: str = "utf-8") -> str:
    return prompt_template_path(filename).read_text(encoding=encoding)
