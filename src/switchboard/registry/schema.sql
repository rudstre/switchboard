-- switchboard registry, schema version 4.
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS experiments (
    id            INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    run_no        INTEGER NOT NULL,
    spec_text     TEXT NOT NULL,
    spec_json     TEXT NOT NULL,     -- validated spec (non-identity fields may be updated on resubmit)
    spec_dir      TEXT NOT NULL,
    spec_hash     TEXT NOT NULL,
    identity_json TEXT NOT NULL,     -- canonical identity fields, used for diffs on refusal
    code_id       TEXT,              -- pinned before any dispatch; NULL when the spec defines no code identity
    code_id_source TEXT,             -- 'evaluated:<backend>' | 'explicit'
    policy        TEXT NOT NULL DEFAULT 'manual',  -- manual | local-first | local-only | hpc-only
    created_at    REAL NOT NULL,
    UNIQUE (name, run_no)
);

CREATE TABLE IF NOT EXISTS experiment_envs (
    experiment_id INTEGER NOT NULL REFERENCES experiments(id),
    platform      TEXT NOT NULL,
    env_id        TEXT NOT NULL,
    pinned_at     REAL NOT NULL,
    PRIMARY KEY (experiment_id, platform)
);

CREATE TABLE IF NOT EXISTS tasks (
    id                  INTEGER PRIMARY KEY,
    experiment_id       INTEGER NOT NULL REFERENCES experiments(id),
    idx                 INTEGER NOT NULL,
    key                 TEXT NOT NULL,
    params_json         TEXT NOT NULL,
    seed                INTEGER,
    cpus                INTEGER NOT NULL,
    mem_mb              INTEGER NOT NULL,
    runtime_s           REAL,
    assigned_backend    TEXT,
    state               TEXT NOT NULL,
    note                TEXT,          -- latest admission/dispatch reason, shown by `explain`
    route_note          TEXT,          -- latest routing decision for auto-routed experiments
    accepted_attempt_id TEXT,
    retries_used        INTEGER NOT NULL DEFAULT 0,
    cancel_requested    INTEGER NOT NULL DEFAULT 0,  -- user asked; otherwise a CANCELLED runner was interrupted
    updated_at          REAL NOT NULL,
    UNIQUE (experiment_id, idx),
    UNIQUE (experiment_id, key)
);
CREATE INDEX IF NOT EXISTS tasks_by_state ON tasks (state, assigned_backend);

CREATE TABLE IF NOT EXISTS launches (
    id           TEXT PRIMARY KEY,
    backend      TEXT NOT NULL,
    external_id  TEXT,
    state        TEXT NOT NULL,      -- SUBMITTING | ACCEPTED | ABANDONED
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS attempts (
    id            TEXT PRIMARY KEY,
    task_id       INTEGER NOT NULL REFERENCES tasks(id),
    launch_id     TEXT NOT NULL REFERENCES launches(id),
    array_idx     INTEGER,           -- index within a Slurm array launch
    backend       TEXT NOT NULL,
    platform      TEXT NOT NULL,
    state         TEXT NOT NULL,
    reason        TEXT,
    exit_code     INTEGER,
    code_id       TEXT,
    env_id        TEXT,
    attempt_dir   TEXT NOT NULL,
    created_at    REAL NOT NULL,
    started_at    REAL,
    ended_at      REAL,
    wall_s        REAL,
    cpu_s         REAL,
    peak_rss_mb   REAL,
    absent_since  REAL,              -- first time the backend reported no trace of it
    unknown_since REAL
);
-- The controller never knowingly runs two attempts of one task at once.
CREATE UNIQUE INDEX IF NOT EXISTS one_active_attempt_per_task
    ON attempts (task_id) WHERE state IN ('SUBMITTING', 'SUBMITTED', 'RUNNING', 'UNKNOWN');
CREATE INDEX IF NOT EXISTS attempts_by_state ON attempts (state, backend);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY,
    ts         REAL NOT NULL,
    entity     TEXT NOT NULL,        -- experiment | task | attempt | launch | control
    entity_id  TEXT NOT NULL,
    from_state TEXT,
    to_state   TEXT,
    reason     TEXT,
    actor      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_by_entity ON events (entity, entity_id);

CREATE TABLE IF NOT EXISTS control (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Whether a backend may run an experiment's tasks: identities checked there before routing to it.
CREATE TABLE IF NOT EXISTS experiment_backends (
    experiment_id INTEGER NOT NULL REFERENCES experiments(id),
    backend       TEXT NOT NULL,
    ok            INTEGER NOT NULL,
    detail        TEXT,
    checked_at    REAL NOT NULL,
    PRIMARY KEY (experiment_id, backend)
);
