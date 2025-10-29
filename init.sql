CREATE TABLE events (
    event_id UUID PRIMARY KEY,
    occurred_at TIMESTAMPTZ,
    user_id TEXT,
    event_type TEXT,
    properties JSONB
)