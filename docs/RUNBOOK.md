# Runbook

## Deploy
On VPS (root):
- `tg-agent-deploy`

## Service status
- `systemctl status tg-agent --no-pager -l`
- `journalctl -u tg-agent -n 100 --no-pager`

## DB checks
- `sqlite3 /opt/tg-agent/data/agent.db "select count(*) from messages;"`
- `sqlite3 /opt/tg-agent/data/agent.db "select id, ts_utc, chat_id, chat_type, username, substr(text,1,60) from messages order by id desc limit 20;"`
