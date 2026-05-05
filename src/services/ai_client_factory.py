import os

from src.services.agentrouter_client import AgentRouterClient
from src.services.openrouter_client import OpenRouterClient


def create_ai_client():
    provider = os.getenv("AI_PROVIDER", "openrouter").strip().lower()

    if provider in {"agentrouter", "agent_router", "agent-router"}:
        return AgentRouterClient()

    return OpenRouterClient()
