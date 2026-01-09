# app/entities_engine.py
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# Project root = parent of /app
BASE_DIR = Path(__file__).resolve().parent.parent
ENTITIES_PATH = BASE_DIR / "config" / "entities.yaml"


@dataclass(frozen=True)
class EntityMatch:
    entity_type: str
    entity_value: str
    entity_raw: str
    confidence: float
    extractor: str


class EntitiesValidationError(RuntimeError):
    pass


def load_entities(path: Path = ENTITIES_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Entities file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise EntitiesValidationError("Top-level YAML must be a mapping (dict).")
    return data


def validate_entities(data: Dict[str, Any]) -> None:
    if not isinstance(data.get("patterns"), dict):
        raise EntitiesValidationError("patterns must be a mapping (dict).")

    patterns = data["patterns"]
    for etype, rules in patterns.items():
        if not isinstance(etype, str) or not etype.strip():
            raise EntitiesValidationError("entity type key must be a non-empty string")
        if not isinstance(rules, list) or not rules:
            raise EntitiesValidationError(f"patterns.{etype} must be a non-empty list")
        for r in rules:
            if not isinstance(r, dict):
                raise EntitiesValidationError(f"patterns.{etype} entries must be dicts")
            if not isinstance(r.get("name"), str) or not r["name"].strip():
                raise EntitiesValidationError(f"patterns.{etype}.name must be non-empty string")
            if not isinstance(r.get("regex"), str) or not r["regex"].strip():
                raise EntitiesValidationError(f"patterns.{etype}.regex must be non-empty string")
            conf = r.get("confidence", 0.5)
            if not isinstance(conf, (int, float)) or not (0.0 <= float(conf) <= 1.0):
                raise EntitiesValidationError(f"patterns.{etype}.confidence must be 0..1")


@lru_cache(maxsize=1)
def get_entities_data() -> Dict[str, Any]:
    data = load_entities()
    validate_entities(data)
    return data


def _normalize(entity_type: str, value: str) -> str:
    v = (value or "").strip()

    if entity_type in ("azs", "workplace", "sd_ticket"):
        digits = "".join(ch for ch in v if ch.isdigit())
        return digits

    if entity_type == "terminal":
        return v.upper()

    if entity_type == "sd_dt":
        # пока без преобразования в ISO — оставим как есть, потом улучшим
        return v

    return v


def extract_entities(text: str, data: Optional[Dict[str, Any]] = None) -> List[EntityMatch]:
    if not text:
        return []

    if data is None:
        data = get_entities_data()

    extractor_version = str(data.get("extractor", "regex:v1"))
    patterns = data.get("patterns") or {}

    found: List[EntityMatch] = []

    for entity_type, rules in patterns.items():
        for r in (rules or []):
            name = str(r.get("name"))
            regex = str(r.get("regex"))
            confidence = float(r.get("confidence", 0.5))

            try:
                for m in re.finditer(regex, text):
                    # берем первую группу, если есть; иначе whole match
                    raw = m.group(0)
                    val = m.group(1) if m.lastindex and m.lastindex >= 1 else raw

                    # Особый случай: workplace может быть списком "1,2,3"
                    if entity_type == "workplace":
                        # вытаскиваем все 1-2 значные числа из захваченного фрагмента
                        nums = re.findall(r"\b\d{1,2}\b", str(val))
                        if nums:
                            for n in nums:
                                norm = _normalize(entity_type, n)
                                if norm:
                                    found.append(
                                        EntityMatch(
                                            entity_type=entity_type,
                                            entity_value=norm,
                                            entity_raw=raw,
                                            confidence=confidence,
                                            extractor=f"{extractor_version}:{name}",
                                        )
                                    )
                            continue  # важное: не падаем в общий путь

                    norm = _normalize(entity_type, val)
                    if not norm:
                        continue

                    found.append(
                        EntityMatch(
                            entity_type=entity_type,
                            entity_value=norm,
                            entity_raw=raw,
                            confidence=confidence,
                            extractor=f"{extractor_version}:{name}",
                        )
                    )

            except re.error:
                # битый regex -> просто пропускаем
                continue

    return found
