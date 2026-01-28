from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class ParameterConfig:
    key: str
    origin: str
    raw_columns: List[str]
    rules: Dict[str, Any]
    label: Optional[str] = None
    unit: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class DQConfig:
    checks: Dict[str, bool]
    parameters: List[ParameterConfig]

    def parameters_for_origin(self, origin: str) -> List[ParameterConfig]:
        origin_l = origin.lower()
        return [p for p in self.parameters if p.origin.lower() == origin_l]


def load_config(path: str) -> DQConfig:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    parameters: List[ParameterConfig] = []
    for key, details in (data.get("parameters") or {}).items():
        parameters.append(
            ParameterConfig(
                key=key,
                origin=details.get("origin", ""),
                raw_columns=details.get("raw_columns", []) or [],
                rules=details.get("rules", {}) or {},
                label=details.get("label"),
                unit=details.get("unit"),
                notes=details.get("notes"),
            )
        )

    return DQConfig(checks=data.get("checks", {}) or {}, parameters=parameters)
