from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from urllib.request import Request, urlopen

from .runtime_config import LlmConfig


@dataclass(slots=True)
class LlmResponse:
    raw_text: str
    parsed_json: dict | list | None
    model: str


class DeepSeekClient:
    def __init__(self, config: LlmConfig) -> None:
        if not config.enabled:
            raise ValueError("LLM config is disabled")
        self.config = config

    def chat_json(self, system_prompt: str, user_prompt: str) -> LlmResponse:
        payload = {
            "model": self.config.model_name,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {"type": "json_object"},
        }
        data = self._post(payload)
        content = data["choices"][0]["message"]["content"]
        return LlmResponse(
            raw_text=content,
            parsed_json=_extract_json(content),
            model=data.get("model", self.config.model_name),
        )

    def _post(self, payload: dict) -> dict:
        endpoint = self.config.base_url.rstrip("/") + "/chat/completions"
        request = Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.config.api_key}",
            },
            method="POST",
        )
        context = None
        if not self.config.verify_ssl:
            context = ssl._create_unverified_context()
        with urlopen(request, timeout=60, context=context) as response:
            return json.loads(response.read().decode("utf-8"))


def _extract_json(text: str) -> dict | list | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            return None
    return None
