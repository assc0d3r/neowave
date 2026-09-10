PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS securities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exchange TEXT NOT NULL,
  symbol TEXT NOT NULL,
  company TEXT NOT NULL DEFAULT '',
  currency TEXT NOT NULL DEFAULT 'INR',
  sector TEXT NOT NULL DEFAULT '',
  isin TEXT NOT NULL DEFAULT '',
  favourite INTEGER NOT NULL DEFAULT 0 CHECK (favourite IN (0,1)),
  archived INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0,1)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(exchange, symbol)
);

CREATE TABLE IF NOT EXISTS analyses (
  id TEXT PRIMARY KEY,
  security_id INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  lock_id TEXT,
  analysis_cutoff TEXT NOT NULL,
  imported_at TEXT NOT NULL,
  engine_name TEXT NOT NULL DEFAULT '',
  engine_version TEXT NOT NULL DEFAULT '',
  schema_version TEXT NOT NULL DEFAULT '',
  structural_state TEXT NOT NULL DEFAULT 'UNRESOLVED',
  primary_scenario TEXT NOT NULL DEFAULT '',
  confidence REAL,
  evidence_score REAL,
  data_quality_score REAL,
  execution_score REAL,
  mtf_score REAL,
  price REAL,
  price_change_pct REAL,
  levels_json TEXT NOT NULL DEFAULT '{}',
  next_evidence_json TEXT NOT NULL DEFAULT '[]',
  delta_json TEXT NOT NULL DEFAULT '{}',
  source_hash TEXT NOT NULL UNIQUE,
  raw_json TEXT NOT NULL,
  validation_status TEXT NOT NULL,
  validation_json TEXT NOT NULL DEFAULT '{}',
  prior_analysis_id TEXT REFERENCES analyses(id),
  visibility TEXT NOT NULL DEFAULT 'private' CHECK(visibility IN ('private','share','published')),
  is_latest INTEGER NOT NULL DEFAULT 1 CHECK (is_latest IN (0,1))
);
CREATE INDEX IF NOT EXISTS idx_analyses_security_cutoff ON analyses(security_id, analysis_cutoff DESC);
CREATE INDEX IF NOT EXISTS idx_analyses_latest ON analyses(is_latest, security_id);
CREATE INDEX IF NOT EXISTS idx_analyses_confidence ON analyses(confidence DESC);

CREATE TABLE IF NOT EXISTS analysis_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  analysis_id TEXT NOT NULL REFERENCES analyses(id) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL,
  ts TEXT NOT NULL,
  stage TEXT NOT NULL,
  status TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_analysis ON analysis_events(analysis_id, ordinal);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  exchange TEXT NOT NULL,
  symbol TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  output_dir TEXT,
  analysis_id TEXT,
  error_code TEXT,
  error_detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC);

CREATE TABLE IF NOT EXISTS watchlists (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watchlist_items (
  watchlist_id INTEGER NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
  security_id INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  added_at TEXT NOT NULL,
  PRIMARY KEY(watchlist_id, security_id)
);

CREATE TABLE IF NOT EXISTS user_preferences (
  user_key TEXT PRIMARY KEY,
  theme TEXT NOT NULL DEFAULT 'glass-intelligence',
  motion INTEGER NOT NULL DEFAULT 1 CHECK(motion IN (0,1)),
  settings_json TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS forecast_outcomes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  analysis_id TEXT NOT NULL UNIQUE REFERENCES analyses(id) ON DELETE CASCADE,
  forecast_id TEXT,
  status TEXT NOT NULL DEFAULT 'OPEN',
  target_achieved INTEGER,
  invalidated INTEGER,
  mfe_pct REAL,
  mae_pct REAL,
  days_to_resolution REAL,
  resolved_at TEXT,
  outcome_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL,
  request_id TEXT NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL DEFAULT '',
  result TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts DESC);
