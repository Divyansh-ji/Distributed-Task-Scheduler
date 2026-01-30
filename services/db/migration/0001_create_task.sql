CREATE TABLE IF NOT EXISTS tasks (
  id           TEXT PRIMARY KEY,
  type         TEXT NOT NULL,
  payload      TEXT NOT NULL,
  status       TEXT NOT NULL,          -- queued|running|success|failed|dead
  scheduled_at TIMESTAMPTZ NOT NULL,
  started_at   TIMESTAMPTZ,
  finished_at  TIMESTAMPTZ,
  max_retries  INT NOT NULL DEFAULT 0,
  attempts     INT NOT NULL DEFAULT 0,
  last_error   TEXT
);