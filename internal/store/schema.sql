-- Applied idempotently at startup (store.ApplySchema). See docs/shared-context.md.

-- Raw log: one row per published Open Responses item, stored untouched.
CREATE TABLE IF NOT EXISTS events (
    id          bigserial PRIMARY KEY,
    session     text        NOT NULL,
    master      text        NOT NULL,
    seq         bigint      NOT NULL,  -- per-master sequence from the tap
    item_id     text        NOT NULL,
    item        jsonb       NOT NULL,
    stream_seq  bigint      NOT NULL,  -- JetStream stream sequence
    received_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE events ADD COLUMN IF NOT EXISTS published_at   timestamptz;  -- JetStream store time
ALTER TABLE events ADD COLUMN IF NOT EXISTS classified_at  timestamptz;  -- all entries processed
ALTER TABLE events ADD COLUMN IF NOT EXISTS classify_error text;         -- set if given up
ALTER TABLE events ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0; -- failed tries
-- tap: the master's own output; mcp: published by the MCP server (share_context). seq is
-- per (master, via).
ALTER TABLE events ADD COLUMN IF NOT EXISTS via text NOT NULL DEFAULT 'tap';
-- Snapshots of a message still being written have rev 1, 2, ...; the finished item has NULL.
-- An item is stored once per rev.
ALTER TABLE events ADD COLUMN IF NOT EXISTS rev int;
ALTER TABLE events DROP CONSTRAINT IF EXISTS events_master_item_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS events_item_rev ON events (master, item_id, coalesce(rev, 0));

CREATE INDEX IF NOT EXISTS events_session_stream_seq ON events (session, stream_seq);
-- The work queue: unprocessed events per session, in order (consumer workers).
CREATE INDEX IF NOT EXISTS events_pending ON events (session, stream_seq) WHERE classified_at IS NULL;

-- Sessions: the task and the topics it is split into, set by whoever starts the session.
-- Topics are {"name": "description or null"}; the classifier prefers them over new ones.
CREATE TABLE IF NOT EXISTS sessions (
    session    text PRIMARY KEY,
    task       text,
    topics     jsonb       NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Entries: the parts of an item (paragraphs, list items, a code block, a tool exchange)
-- that shared context is made of. Each keeps provenance back to its event.
CREATE TABLE IF NOT EXISTS entries (
    id            bigserial PRIMARY KEY,
    event_id      bigint   NOT NULL REFERENCES events (id),
    part          smallint NOT NULL,  -- position within the item
    session       text     NOT NULL,
    master        text     NOT NULL,
    seq           bigint   NOT NULL,
    stream_seq    bigint   NOT NULL,
    text          text     NOT NULL,
    topic         text,
    kind          text,
    score         real CHECK (score BETWEEN 1 AND 5),       -- relevance for other masters
    confidence    real CHECK (confidence BETWEEN 0 AND 1),  -- of the score (decision models)
    -- pending: not classified yet. active: current. superseded: replaced by its own master.
    -- resolved: an open question that was answered. duplicate: restated an existing entry.
    status        text NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'active', 'superseded', 'resolved', 'duplicate')),
    -- How this entry relates to an existing one on its topic, and which one.
    relation      text CHECK (relation IN ('new', 'restates', 'replaces', 'answers', 'contradicts')),
    related_id    bigint REFERENCES entries (id),
    closed_by     bigint REFERENCES entries (id),  -- the entry that superseded or resolved this one
    classified_at timestamptz,
    UNIQUE (event_id, part)
);
ALTER TABLE entries ADD COLUMN IF NOT EXISTS relation_prob real;  -- the model's probability
-- What the models were asked and answered, and what decide() made of it (for the dashboard).
ALTER TABLE entries ADD COLUMN IF NOT EXISTS trace jsonb;
-- Shared deliberately with share_context: kind (and topic, if given) come from the master.
ALTER TABLE entries ADD COLUMN IF NOT EXISTS declared boolean NOT NULL DEFAULT false;
-- An entry is a part of an item, whichever event (a streamed snapshot or the finished item)
-- created it first.
ALTER TABLE entries ADD COLUMN IF NOT EXISTS item_id text;
UPDATE entries x SET item_id = e.item_id FROM events e
WHERE x.item_id IS NULL AND e.id = x.event_id;
CREATE UNIQUE INDEX IF NOT EXISTS entries_item_part ON entries (master, item_id, part);

CREATE INDEX IF NOT EXISTS entries_active ON entries (session, topic) WHERE status = 'active';

-- Kinds (classifier.Kinds); replaced on every start so existing databases get new ones.
ALTER TABLE entries DROP CONSTRAINT IF EXISTS entries_kind_check;
ALTER TABLE entries ADD CONSTRAINT entries_kind_check
    CHECK (kind IN ('fact', 'decision', 'hypothesis', 'open_question', 'artifact', 'narration'));

-- Milestone 2 tagged whole events and kept a context_view table; tags now live on entries.
DROP VIEW IF EXISTS context_items;
DROP VIEW IF EXISTS context_ranked;
DROP VIEW IF EXISTS context_entries;
DROP TABLE IF EXISTS context_view;
DROP INDEX IF EXISTS events_context;
ALTER TABLE events
    DROP COLUMN IF EXISTS topic,
    DROP COLUMN IF EXISTS kind,
    DROP COLUMN IF EXISTS final,
    DROP COLUMN IF EXISTS score,
    DROP COLUMN IF EXISTS confidence;

-- Visible entries: active, and worth showing. Relevance >= 3 ("somewhat useful"), or 2.5 for
-- open questions, which score lower by nature; hypotheses and narration are never shown. Keep
-- in sync with classifier.Visible.
CREATE VIEW context_entries AS
SELECT * FROM entries
WHERE status = 'active'
  AND ((kind = 'open_question' AND score >= 2.5)
       OR (kind NOT IN ('hypothesis', 'open_question', 'narration') AND score >= 3));

-- Ranked by relevance, then recency, per topic; open questions are ranked separately from the
-- rest of their topic (handoffs list them in their own section).
CREATE VIEW context_ranked AS
SELECT id, session, topic, kind, score, confidence, master, seq, stream_seq, text,
       relation, related_id, declared,
       row_number() OVER (
           PARTITION BY session, topic, kind = 'open_question'
           ORDER BY score DESC, stream_seq DESC
       ) AS topic_rank
FROM context_entries;

-- Change notifications for live views (wetink dashboard): LISTEN wetink_changes.
CREATE OR REPLACE FUNCTION wetink_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('wetink_changes', json_build_object(
        'table', TG_TABLE_NAME, 'op', TG_OP, 'session', NEW.session)::text);
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS events_notify ON events;
CREATE TRIGGER events_notify AFTER INSERT OR UPDATE ON events
    FOR EACH ROW EXECUTE FUNCTION wetink_notify();
DROP TRIGGER IF EXISTS entries_notify ON entries;
CREATE TRIGGER entries_notify AFTER INSERT OR UPDATE ON entries
    FOR EACH ROW EXECUTE FUNCTION wetink_notify();
DROP TRIGGER IF EXISTS sessions_notify ON sessions;
CREATE TRIGGER sessions_notify AFTER INSERT OR UPDATE ON sessions
    FOR EACH ROW EXECUTE FUNCTION wetink_notify();
