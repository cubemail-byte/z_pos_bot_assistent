# app/rules_engine.py
"""
Rules Engine v1 (no AI): load + validate rules.yaml and classify plain text.

Usage (from /app):
  python rules_engine.py validate
  python rules_engine.py test
  python rules_engine.py classify "АСУТП не видит терминал"
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml


# Project root = parent of /app
BASE_DIR = Path(__file__).resolve().parent.parent
RULES_PATH = BASE_DIR / "config" / "rules.yaml"


@dataclass(frozen=True)
class MatchResult:
    code: str
    rule_id: str
    priority: int
    weight: float
    hint_symptom: str
    matched_include: str


class RulesValidationError(RuntimeError):
    pass


def load_rules(path: Path = RULES_PATH) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Rules file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise RulesValidationError("Top-level YAML must be a mapping (dict).")
    return data


def validate_rules(data: Dict[str, Any]) -> Tuple[int, int]:
    """
    Validate rules structure.
    Returns: (rules_count, codes_count)
    Raises: RulesValidationError on problems.
    """
    taxonomy = data.get("problem_taxonomy") or {}
    codes = taxonomy.get("codes") or {}
    if not isinstance(codes, dict) or not codes:
        raise RulesValidationError("problem_taxonomy.codes must be a non-empty mapping of code -> description.")

    rules = data.get("problem_rules") or []
    if not isinstance(rules, list):
        raise RulesValidationError("problem_rules must be a list.")

    seen_ids = set()

    for i, r in enumerate(rules):
        if not isinstance(r, dict):
            raise RulesValidationError(f"Rule #{i} must be a mapping (dict).")

        rid = r.get("id")
        if not rid or not isinstance(rid, str):
            raise RulesValidationError(f"Rule #{i} has no valid 'id' (must be non-empty string).")
        if rid in seen_ids:
            raise RulesValidationError(f"Duplicate rule id: {rid}")
        seen_ids.add(rid)

        enabled = r.get("enabled")
        if not isinstance(enabled, bool):
            raise RulesValidationError(f"Rule {rid}: 'enabled' must be boolean true/false.")

        code = r.get("code")
        if not code or not isinstance(code, str):
            raise RulesValidationError(f"Rule {rid}: 'code' must be non-empty string.")
        if code not in codes:
            raise RulesValidationError(f"Rule {rid}: code '{code}' is not present in problem_taxonomy.codes.")

        priority = r.get("priority")
        if not isinstance(priority, int):
            raise RulesValidationError(f"Rule {rid}: 'priority' must be integer.")

        weight = r.get("weight")
        if not isinstance(weight, (int, float)):
            raise RulesValidationError(f"Rule {rid}: 'weight' must be number.")
        weight = float(weight)
        if not (0.0 <= weight <= 1.0):
            raise RulesValidationError(f"Rule {rid}: 'weight' must be between 0.0 and 1.0.")

        inc = r.get("include_any")
        exc = r.get("exclude_any")

        if not isinstance(inc, list) or len(inc) == 0:
            raise RulesValidationError(f"Rule {rid}: include_any must be a non-empty list.")
        if not all(isinstance(x, str) and x.strip() for x in inc):
            raise RulesValidationError(f"Rule {rid}: include_any must contain only non-empty strings.")

        if exc is None:
            raise RulesValidationError(f"Rule {rid}: exclude_any must be present (can be empty list).")
        if not isinstance(exc, list):
            raise RulesValidationError(f"Rule {rid}: exclude_any must be a list.")
        if not all(isinstance(x, str) and x.strip() for x in exc):
            raise RulesValidationError(f"Rule {rid}: exclude_any must contain only non-empty strings (or be []).")

    return (len(rules), len(codes))


def _sorted_rules(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Sort by priority desc, then weight desc, stable by file order
    def sort_key(r: Dict[str, Any]):
        return (int(r.get("priority", 0)), float(r.get("weight", 0.0)))

    enabled_rules = [r for r in rules if r.get("enabled", True) is True]
    return sorted(enabled_rules, key=sort_key, reverse=True)


def classify_text(text: str, data: Dict[str, Any]) -> Optional[MatchResult]:
    """
    Returns best matching rule result or None.
    Matching logic:
      - include_any: at least one regex must match
      - exclude_any: none must match
      - choose by priority desc, then weight desc (stable by file order)
    """
    if not text:
        return None

    rules: List[Dict[str, Any]] = data.get("problem_rules") or []
    candidates = _sorted_rules(rules)

    for r in candidates:
        include_any = r.get("include_any") or []
        exclude_any = r.get("exclude_any") or []

        matched_pat = None
        for pat in include_any:
            try:
                if re.search(pat, text):
                    matched_pat = pat
                    break
            except re.error:
                # Bad regex in rule -> treat as non-match
                matched_pat = None
                break

        if not matched_pat:
            continue

        excluded = False
        for pat in exclude_any:
            try:
                if re.search(pat, text):
                    excluded = True
                    break
            except re.error:
                # Bad exclude regex -> ignore this exclude pattern
                continue

        if excluded:
            continue

        return MatchResult(
            code=str(r.get("code")),
            rule_id=str(r.get("id")),
            priority=int(r.get("priority", 0)),
            weight=float(r.get("weight", 0.0)),
            hint_symptom=str(r.get("hint_symptom", "")),
            matched_include=str(matched_pat),
        )

    return None


def _default_tests() -> Sequence[str]:
    return [
        "АСУТП не видит терминал на кассе 3",
        "Итоги не бьются, нужна сверка",
        "Терминал уходит в цикличные перезагрузки (reboot loop)",
        "Нет доступа, не отображается заявка у сотрудника",
        "Перезагрузил и заработало (проверка исключения)",
    ]


def cmd_validate() -> int:
    data = load_rules()
    rules_count, codes_count = validate_rules(data)
    print(f"OK: ruleset_version={data.get('ruleset_version')} rules={rules_count} codes={codes_count}")
    return 0


def cmd_test() -> int:
    data = load_rules()
    rules_count, codes_count = validate_rules(data)
    print(f"OK: ruleset_version={data.get('ruleset_version')} rules={rules_count} codes={codes_count}")
    for t in _default_tests():
        res = classify_text(t, data)
        print("-" * 80)
        print("TEXT:", t)
        print("RESULT:", res)
    return 0


def cmd_classify(args: List[str]) -> int:
    if not args:
        print("Usage: python rules_engine.py classify <text>")
        return 2
    text = " ".join(args)
    data = load_rules()
    validate_rules(data)
    res = classify_text(text, data)
    print(res)
    return 0


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage:")
        print("  python rules_engine.py validate")
        print("  python rules_engine.py test")
        print('  python rules_engine.py classify "text..."')
        return 2

    cmd = argv[1].lower().strip()

    try:
        if cmd == "validate":
            return cmd_validate()
        if cmd == "test":
            return cmd_test()
        if cmd == "classify":
            return cmd_classify(argv[2:])
        print(f"Unknown command: {cmd}")
        return 2
    except (FileNotFoundError, RulesValidationError) as e:
        print("ERROR:", e)
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
