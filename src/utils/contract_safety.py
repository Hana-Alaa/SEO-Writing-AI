import inspect
import logging
from typing import Callable, Any, Dict

logger = logging.getLogger(__name__)

class PipelineContractError(TypeError):
    """
    Raised when a service call does not match the expected signature.
    This is treated as a fatal technical error that bypasses all workflow retries.
    """
    pass

def validate_service_call(target_callable: Callable, **kwargs: Any) -> None:
    """
    Performs a preflight check to verify that the provided keyword arguments 
    match the target method's signature.
    
    Raises:
        PipelineContractError: If there is a signature mismatch (missing or unexpected args).
    """
    name = getattr(target_callable, "__qualname__", str(target_callable))
    try:
        sig = inspect.signature(target_callable)
        # Attempt to bind the provided kwargs to the signature
        sig.bind(**kwargs)
    except TypeError as e:
        error_msg = f"CONTRACT VIOLATION in '{name}': {e}"
        logger.error(error_msg)
        # Wrap the standard TypeError in our custom contract error for fail-fast identification
        raise PipelineContractError(error_msg) from e

def is_signature_mismatch(error: Exception) -> bool:
    """
    Helper to identify if a raw TypeError is signature-related.
    Used for the narrow fallback in the workflow controller.
    """
    if not isinstance(error, TypeError):
        return False
    msg = str(error).lower()
    return (
        "unexpected keyword argument" in msg or 
        "missing a required" in msg or 
        "positional argument" in msg
    )
