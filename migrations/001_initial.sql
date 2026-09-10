CREATE TABLE users (
 id uuid PRIMARY KEY, email text NOT NULL UNIQUE, password_hash text NOT NULL,
 role text NOT NULL DEFAULT 'user' CHECK(role IN ('user','admin')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE endpoints (
 id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id), url text NOT NULL,
 secret_ciphertext text NOT NULL, event_types jsonb NOT NULL,
 enabled boolean NOT NULL DEFAULT true, created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX endpoints_owner ON endpoints(user_id,created_at);
CREATE TABLE events (
 id uuid PRIMARY KEY, user_id uuid NOT NULL REFERENCES users(id), type text NOT NULL,
 body text NOT NULL, idempotency_key text NOT NULL, request_hash text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE(user_id,idempotency_key)
);
CREATE TABLE deliveries (
 id uuid PRIMARY KEY, event_id uuid NOT NULL REFERENCES events(id), endpoint_id uuid NOT NULL REFERENCES endpoints(id),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','queued','processing','retry','delivered','dead','cancelled')),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts >= 0),
 cycle_attempts integer NOT NULL DEFAULT 0 CHECK(cycle_attempts >= 0),
 generation uuid, lease_until timestamptz, not_before timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_error text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), UNIQUE(event_id,endpoint_id)
);
CREATE INDEX deliveries_schedule ON deliveries(not_before,lease_until) WHERE status IN ('pending','retry','queued','processing');
CREATE TABLE attempts (
 id uuid PRIMARY KEY, delivery_id uuid NOT NULL REFERENCES deliveries(id), attempt_no integer NOT NULL,
 result text NOT NULL DEFAULT 'running', status_code integer, error text,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 UNIQUE(delivery_id,attempt_no)
);
CREATE TABLE worker_heartbeats (name text PRIMARY KEY, seen_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE demo_received_events (
 scenario_id uuid NOT NULL, event_id uuid NOT NULL, body_hash text NOT NULL,
 received_count integer NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(scenario_id,event_id)
);
