# tg-agent — Schema

Этот документ описывает текущую схему хранения данных и типовые запросы для аналитики.

---

## Таблица `messages` — raw-сообщения Telegram

Таблица `messages` содержит **сырые (raw) данные всех сообщений** из эскалационных Telegram-чатов.  
Это **источник истины**, на базе которого строится дальнейшая классификация, аналитика и SLA-метрики.

### Назначение
- Надёжно зафиксировать **каждое событие** в чате
- Сохранить **максимум контекста**, не принимая продуктовых решений
- Обеспечить возможность:
  - повторной классификации,
  - анализа реакции,
  - построения временных цепочек,
  - выявления типовых и массовых проблем

Таблица **не содержит логики**, только факты.

---

## Структура `messages`

### Идентификация сообщения

| Поле | Тип | Описание |
|---|---|---|
| `id` | INTEGER PK | Внутренний идентификатор записи |
| `ts_utc` | TEXT | Время получения сообщения (UTC, ISO-8601) |
| `tg_message_id` | INTEGER | ID сообщения в Telegram (уникален в рамках чата) |
| `chat_id` | INTEGER | ID чата в Telegram |
| `chat_type` | TEXT | Тип чата (`private`, `group`, `supergroup`) |
| `chat_alias` | TEXT | Человекочитаемый идентификатор чата (из `config.yaml`) |

### Автор сообщения

| Поле | Тип | Описание |
|---|---|---|
| `from_id` | INTEGER | Telegram user_id автора |
| `username` | TEXT | Telegram username |
| `from_display` | TEXT | Отображаемое имя (first + last) |
| `from_role` | TEXT | Роль пользователя (по справочнику `config.yaml`/`users`) |

> В личных чатах Telegram норма: `from_id == chat_id`.

### Контент сообщения

| Поле | Тип | Описание |
|---|---|---|
| `text` | TEXT | Текст сообщения или подпись (caption) |
| `content_type` | TEXT | Тип сообщения (`text`, `photo`, `video`, `document`, `service`, и т.д.) |
| `has_media` | INTEGER | Флаг наличия медиа (0 / 1) |
| `service_action` | TEXT | Тип сервисного события (`new_chat_members`, `left_chat_member`, `pinned_message`) |

> **Медиа-файлы не сохраняются** — только факт их наличия.

### Reply (ответы)

| Поле | Тип | Описание |
|---|---|---|
| `reply_to_tg_message_id` | INTEGER | Telegram ID сообщения, на которое был ответ |
| `reply_to_from_id` | INTEGER | user_id автора исходного сообщения |
| `reply_to_username` | TEXT | username автора исходного сообщения |
| `reply_kind` | TEXT | Тип reply: `response` (банк/сервис), `escalation` (клиент), `NULL` если не reply |

Используется для:
- анализа реакции,
- определения “кто кому ответил”,
- расчёта времени первой реакции (TTFR).

### Forward (пересылки)

| Поле | Тип | Описание |
|---|---|---|
| `forward_from_id` | INTEGER | ID источника пересылки (user/chat/channel) |
| `forward_from_name` | TEXT | Имя источника пересылки |

Определяется через `forward_origin` (Bot API 6+).  
Если пользователь пересылает “как копию”, поля могут быть `NULL`.

### Метаданные

| Поле | Тип | Описание |
|---|---|---|
| `edited_ts_utc` | TEXT | Время редактирования сообщения (если было) |
| `raw_json` | TEXT | Полный JSON-дамп сообщения Telegram |

`raw_json` используется:
- для отладки,
- для повторного парсинга,
- как защита от потери данных при изменении логики.

---

## Гарантии и инварианты

- Каждое сообщение пишется **один раз**
- Данные **не перезаписываются**
- Таблица **не зависит от логики классификации**
- Возможна повторная обработка без изменения ingestion-слоя

---

## Связанные таблицы

### `message_classification` — слой классификации

## Таблица: message_classification

Таблица содержит результат классификации сообщений.

Назначение:
- хранение текущего статуса классификации
- поддержка online и offline обработки
- отделение raw-данных от аналитики

### Структура

| Поле | Тип | Описание |
|-----|----|---------|
| id | INTEGER PK | Уникальный идентификатор |
| message_id | INTEGER UNIQUE | Ссылка на messages.id |
| chat_id | INTEGER | Идентификатор чата |
| tg_message_id | INTEGER | Идентификатор сообщения в Telegram |
| problem_domain | TEXT | Домен проблемы (v1: PROBLEM / UNCLASSIFIED) |
| problem_symptom | TEXT | Код проблемы (например TOTAL_MISMATCH) |
| rule_id | TEXT | Идентификатор правила |
| confidence | REAL | Уверенность классификации |
| ruleset_version | TEXT | Версия rules.yaml |
| is_unclassified | INTEGER | 1 — не классифицировано |
| classified_at_utc | TEXT | Время классификации |
| created_at_utc | TEXT | Время создания записи |
| updated_at_utc | TEXT | Время обновления записи |

### Примечания
- строка создаётся всегда при ingestion
- отсутствие классификации — допустимое состояние
- таблица допускает повторную обработку сообщений


---

# Типовые запросы к таблице `messages`

Запросы ориентированы на:
- контроль реакции на эскалации,
- анализ повторяемости проблем,
- выявление массовых инцидентов,
- диагностику SLA-отклонений.

## 1) Последние сообщения по чату

```sql
SELECT id, ts_utc, from_display, text
FROM messages
WHERE chat_alias = 'tatneft_escalation'
ORDER BY id DESC
LIMIT 50;
```

## 2) Сообщения без reply-ответа (потенциальные “висящие”)

```sql
SELECT m.id, m.tg_message_id, m.ts_utc, m.from_display, m.text
FROM messages m
LEFT JOIN messages r
  ON r.chat_id = m.chat_id
 AND r.reply_to_tg_message_id = m.tg_message_id
WHERE m.chat_type IN ('group', 'supergroup')
  AND r.id IS NULL
ORDER BY m.id DESC
LIMIT 50;
```

## 3) Время первой реакции (TTFR)

```sql
SELECT
  m.id AS src_id,
  m.ts_utc AS src_ts,
  MIN(r.ts_utc) AS first_reply_ts,
  CAST((julianday(MIN(r.ts_utc)) - julianday(m.ts_utc)) * 86400 AS INTEGER) AS ttfr_seconds
FROM messages m
JOIN messages r
  ON r.chat_id = m.chat_id
 AND r.reply_to_tg_message_id = m.tg_message_id
WHERE m.chat_type IN ('group', 'supergroup')
GROUP BY m.id
ORDER BY ttfr_seconds DESC
LIMIT 20;
```

## 4) Кто кому отвечает

```sql
SELECT
  m.from_display AS original_author,
  r.from_display AS responder,
  COUNT(*) AS replies_count
FROM messages m
JOIN messages r
  ON r.chat_id = m.chat_id
 AND r.reply_to_tg_message_id = m.tg_message_id
GROUP BY original_author, responder
ORDER BY replies_count DESC;
```

## 5) Сообщения с медиа (факт)

```sql
SELECT id, ts_utc, from_display, content_type, length(text) AS text_len
FROM messages
WHERE has_media = 1
ORDER BY id DESC
LIMIT 50;
```

## 6) Форварды (источники)

```sql
SELECT forward_from_name, COUNT(*) AS forwarded_count
FROM messages
WHERE forward_from_name IS NOT NULL
GROUP BY forward_from_name
ORDER BY forwarded_count DESC;
```

## 7) Пики активности по часам

```sql
SELECT substr(ts_utc, 1, 13) AS hour, COUNT(*) AS messages_count
FROM messages
WHERE chat_alias = 'tatneft_escalation'
GROUP BY hour
ORDER BY messages_count DESC;
```

## 8) Повторяющиеся тексты (грубая симптоматика)

```sql
SELECT substr(text, 1, 100) AS text_sample, COUNT(*) AS cnt
FROM messages
WHERE length(text) > 20
GROUP BY text_sample
ORDER BY cnt DESC
LIMIT 20;
```

## 9) Отредактированные сообщения

```sql
SELECT id, ts_utc, edited_ts_utc, from_display, text
FROM messages
WHERE edited_ts_utc IS NOT NULL
ORDER BY edited_ts_utc DESC;
```

## 10) UNCLASSIFIED (когда подключена `message_classification`)

```sql
SELECT m.id, m.text, c.problem_domain, c.is_unclassified
FROM messages m
LEFT JOIN message_classification c ON c.message_id = m.id
WHERE c.is_unclassified = 1
ORDER BY m.id DESC
LIMIT 50;
```
