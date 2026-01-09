import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
import json

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

from storage import init_db, ingest_raw_and_classify
from rules_engine import load_rules, classify_text

from storage import init_db, ingest_raw_and_classify, get_message_entities
from storage import get_message_entities_multi, lookup_terminal_directory_by_azs_wp



RULES_DATA = load_rules()
RULESET_VERSION = str(RULES_DATA.get("ruleset_version", "0"))
RESPONSE_ROLES = {"bank", "service_coordinator", "service_support"}

def load_config(project_root: Path) -> dict:
    config_path = project_root / "config.yaml"
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


def build_user_role_index(cfg: dict) -> dict[int, str]:
    roles: dict[int, str] = {}
    for item in cfg.get("users", []) or []:
        if not isinstance(item, dict):
            continue
        user_id = item.get("user_id")
        role = item.get("role")
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            continue
        if role:
            roles[uid] = str(role)
    return roles


def chat_alias_for(chat_id: int, cfg: dict) -> str | None:
    for c in cfg.get("chats", []):
        try:
            if int(c.get("chat_id")) == int(chat_id):
                return c.get("alias")
        except Exception:
            continue
    return None


def message_to_raw_json(message: Message) -> str:
    try:
        data = message.model_dump()
    except Exception:
        try:
            data = message.to_python()
        except Exception:
            data = {"repr": repr(message)}
    return json.dumps(data, ensure_ascii=False)


def should_send_reply(cfg: dict) -> bool:
    reply_cfg = cfg.get("reply") or {}
    return bool(reply_cfg.get("enabled", False))


def build_reply_text(cfg: dict, entities: dict[str, str]) -> str:
    reply_cfg = cfg.get("reply") or {}
    include = set(reply_cfg.get("include_entities") or [])

    parts = []
    # KE
    ke = []
    if "azs" in include and entities.get("azs"):
        ke.append(f"АЗС {entities['azs']}")
    if "workplace" in include and entities.get("workplace"):
        ke.append(f"РМ {entities['workplace']}")
    if ke:
        parts.append("KE: " + ", ".join(ke))

    if "tid" in include and entities.get("tid"):
        parts.append(f"TID: {entities['tid']}")
    if "ip" in include and entities.get("ip"):
        parts.append(f"IP: {entities['ip']}")

    return "\n".join(parts).strip()

def build_reply_text_multi(cfg: dict, sqlite_path: str, entities: dict[str, list[str]]) -> str:
    reply_cfg = cfg.get("reply") or {}
    include = set(reply_cfg.get("include_entities") or [])

    azs = (entities.get("azs") or [None])[0]
    wps = sorted({w for w in (entities.get("workplace") or []) if w})

    if not azs or not wps:
        return ""

    lines = []
    # Заголовок KE
    if "azs" in include and "workplace" in include:
        lines.append(f"KE: АЗС {azs}, РМ " + ",".join(wps))
    else:
        # на всякий случай
        lines.append(f"KE: АЗС {azs}")

    found_any_tid = False

    # По каждому РМ — lookup в справочнике, чтобы корректно сопоставить tid/ip
    for wp in wps:
        rows = lookup_terminal_directory_by_azs_wp(sqlite_path, azs, wp)

        # require_unique_match по-хорошему должен быть и тут, но пока: если 1 строка — ок, иначе пропускаем
        if len(rows) != 1:
            continue

        tid, ip, arm = rows[0]
        if tid:
            found_any_tid = True

        parts = [f"РМ {wp}:"]
        if "tid" in include and tid:
            parts.append(f"TID {tid}")
        if "ip" in include and ip:
            parts.append(f"IP {ip}")

        # если по этому РМ вообще нечего показывать — пропускаем строку
        if len(parts) > 1:
            lines.append(" ".join(parts))

    # ВАЖНО: если нет TID ни для одного РМ — не отвечаем вообще
    if not found_any_tid:
        return ""

    return "\n".join(lines).strip()

async def main() -> None:
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set (put it into /opt/tg-agent/.env)")

    project_root = Path(__file__).resolve().parent.parent  # .../app/bot.py -> .../
    cfg = load_config(project_root)
    user_roles = build_user_role_index(cfg)

    sqlite_path_cfg = cfg.get("storage", {}).get("sqlite_path", "data/agent.db")
    sqlite_path = Path(sqlite_path_cfg)
    if not sqlite_path.is_absolute():
        sqlite_path = project_root / sqlite_path  # всегда относительно корня проекта
    sqlite_path = str(sqlite_path)

    reply_in_groups = bool(cfg.get("bot", {}).get("reply_in_groups", False))

    init_db(sqlite_path)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("tg-agent")

    bot = Bot(token=token)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(message: Message):
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return
        await message.answer("Привет! Я жив. Команда: /ping")

    @dp.message(Command("ping"))
    async def ping(message: Message):
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return
        await message.answer("pong")

# ---------------------------------
# on_message ----------------------

    @dp.message()
    async def on_message(message: Message):
        ts_utc = datetime.now(timezone.utc).isoformat()

        chat_id = message.chat.id
        alias = chat_alias_for(chat_id, cfg)

        from_id = message.from_user.id if message.from_user else None
        username = message.from_user.username if message.from_user else None
        from_role = user_roles.get(from_id) if from_id is not None else None

        from_display = None
        if message.from_user:
            first = (message.from_user.first_name or "").strip()
            last = (message.from_user.last_name or "").strip()
            from_display = (first + " " + last).strip() or None

        reply_to_tg_message_id = (
            message.reply_to_message.message_id
            if message.reply_to_message
            else None
        )

        reply_to_from_id = None
        reply_to_username = None
        if message.reply_to_message and message.reply_to_message.from_user:
            reply_to_from_id = message.reply_to_message.from_user.id
            reply_to_username = message.reply_to_message.from_user.username

        reply_kind = None
        if reply_to_tg_message_id is not None:
            if from_role == "client":
                reply_kind = "escalation"
            elif from_role in RESPONSE_ROLES:
                reply_kind = "response"


        # --- NEW: text может быть в caption (фото/док с подписью) ---
        text = message.text or message.caption or ""

        # --- NEW: content_type + has_media без сохранения контента ---
        content_type = getattr(message, "content_type", None) or "other"
        has_media = 1 if content_type in {
            "photo", "video", "document", "audio", "voice", "video_note", "animation", "sticker"
        } else 0

        # --- NEW: service events ---
        service_action = None
        if getattr(message, "new_chat_members", None):
            content_type = "service"
            has_media = 0
            service_action = "new_chat_members"
        elif getattr(message, "left_chat_member", None):
            content_type = "service"
            has_media = 0
            service_action = "left_chat_member"
        elif getattr(message, "pinned_message", None):
            content_type = "service"
            has_media = 0
            service_action = "pinned_message"

        forward_from_id = None
        forward_from_name = None

        origin = getattr(message, "forward_origin", None)
        if origin:
            # В Bot API forward_origin бывает разных типов (user/chat/hidden_user/channel и т.п.)
            # aiogram даёт объект с полями, зависящими от типа. Пробуем аккуратно.
            # 1) Если это forward от пользователя
            user = getattr(origin, "sender_user", None)
            if user:
                forward_from_id = getattr(user, "id", None)
                fn = getattr(user, "first_name", None) or ""
                ln = getattr(user, "last_name", None) or ""
                un = getattr(user, "username", None) or ""
                forward_from_name = (fn + " " + ln).strip() or un or None

            # 2) Если это forward из чата/канала
            chat = getattr(origin, "sender_chat", None)
            if chat and not forward_from_name:
                forward_from_id = getattr(chat, "id", None)
                forward_from_name = getattr(chat, "title", None) or getattr(chat, "username", None) or None

            # 3) Если это скрытый пользователь (hidden user)
            hidden = getattr(origin, "sender_user_name", None)
            if hidden and not forward_from_name:
                forward_from_name = hidden

        # Fallback для старых/особых случаев
        if not forward_from_name:
            fwd_user = getattr(message, "forward_from", None)
            if fwd_user:
                forward_from_id = getattr(fwd_user, "id", None)
                fn = getattr(fwd_user, "first_name", None) or ""
                ln = getattr(fwd_user, "last_name", None) or ""
                un = getattr(fwd_user, "username", None) or ""
                forward_from_name = (fn + " " + ln).strip() or un or None

        if not forward_from_name:
            fwd_chat = getattr(message, "forward_from_chat", None)
            if fwd_chat:
                forward_from_id = getattr(fwd_chat, "id", None)
                forward_from_name = getattr(fwd_chat, "title", None) or getattr(fwd_chat, "username", None) or None

        # --- классификация (best-effort) ---
        match = None
        if text:
            res = classify_text(text, RULES_DATA)
            if res:
                match = {
                    "code": res.code,
                    "rule_id": res.rule_id,
                    "weight": res.weight,
                }

        message_id = ingest_raw_and_classify(
            db_path=sqlite_path,
            m={
                "ts_utc": ts_utc,
                "chat_id": chat_id,
                "chat_type": message.chat.type,
                "chat_alias": alias,

                "from_id": from_id,
                "username": username,
                "from_display": from_display,
                "from_role": from_role,

                "text": text,

                "tg_message_id": message.message_id,
                "reply_to_tg_message_id": reply_to_tg_message_id,

                "reply_to_from_id": reply_to_from_id,
                "reply_to_username": reply_to_username,
                "reply_kind": reply_kind,

                "content_type": content_type,
                "has_media": has_media,
                "service_action": service_action,

                "edited_ts_utc": (
                    message.edit_date.astimezone(timezone.utc).isoformat()
                    if message.edit_date
                    else None
                ),

                "forward_from_id": forward_from_id,
                "forward_from_name": forward_from_name,

                "raw_json": message_to_raw_json(message),
            },
            match=match,
            ruleset_version=RULESET_VERSION,
        )


        log.info(
            "saved raw message chat_id=%s tg_message_id=%s alias=%s content_type=%s has_media=%s",
            chat_id,
            message.message_id,
            alias,
            content_type,
            has_media,
        )

        # --- reply / notify (управляется config.yaml, по ролям) ---

        reply_cfg = cfg.get("reply") or {}
        log.info(
            "reply_check enabled=%s mode=%s allowed_roles=%s from_id=%s from_role=%s reply_in_groups=%s",
            reply_cfg.get("enabled", None),
            reply_cfg.get("mode", None),
            reply_cfg.get("allowed_roles", None),
            from_id,
            from_role,
            reply_in_groups,
        )

        if should_send_reply(cfg):
            try:
                reply_cfg = cfg.get("reply") or {}
                mode = str(reply_cfg.get("mode", "engineer_chat"))

                entities = get_message_entities_multi(sqlite_path, message_id)

                required = set((cfg.get("reply") or {}).get("require_entities") or [])
                if required:
                    missing = [k for k in required if not entities.get(k)]
                    if missing:
                        log.info("reply_skipped_missing_entities missing=%s entities=%s", missing, entities)
                        return            
                
                reply_text = build_reply_text_multi(cfg, sqlite_path, entities)

                log.info("reply_ready mode=%s reply_text_len=%s entities=%s", mode, len(reply_text or ""), entities)

                if reply_text:
                    if mode == "engineer_chat":
                        engineer_chat_id = reply_cfg.get("engineer_chat_id")
                        if engineer_chat_id:
                            await bot.send_message(int(engineer_chat_id), reply_text)
                    elif mode == "reply":
                        await message.reply(reply_text)
            except Exception:
                log.exception("reply_failed")

        # тихий режим в группах
        if message.chat.type in ("group", "supergroup") and not reply_in_groups:
            return

        # В личке эхо оставляем. В группе до этой строки не дойдём из-за return выше.
        await message.answer(text)


    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
