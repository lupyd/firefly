
DROP SCHEMA public CASCADE;

CREATE SCHEMA public;


CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE OR REPLACE FUNCTION uuidv7() RETURNS uuid
AS $$
  -- Replace the first 48 bits of a uuidv4 with the current
  -- number of milliseconds since 1970-01-01 UTC
  -- and set the "ver" field to 7 by setting additional bits
  select encode(
    set_bit(
      set_bit(
        overlay(uuid_send(gen_random_uuid()) placing
	  substring(int8send((extract(epoch from clock_timestamp())*1000)::bigint) from 3)
	  from 1 for 6),
	52, 1),
      53, 1), 'hex')::uuid;
$$ LANGUAGE sql volatile;


CREATE OR REPLACE FUNCTION now_us() RETURNS bigint AS $$
  SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint;
$$ LANGUAGE sql STABLE;



CREATE TABLE IF NOT EXISTS addresses (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  username VARCHAR NOT NULL,
  device_id SMALLINT NOT NULL,
  fcm_token VARCHAR NOT NULL
);


CREATE UNIQUE INDEX IF NOT EXISTS addresses_by_username ON addresses (username, device_id);



-- settings
-- 1 - public - can join via link
-- 2 - allow history syncing


CREATE TABLE IF NOT EXISTS groups (
    id BIGSERIAL NOT NULL PRIMARY KEY,
    owner VARCHAR NOT NULL,
    state BYTEA NOT NULL,
    description VARCHAR NOT NULL DEFAULT '',
    name VARCHAR NOT NULL DEFAULT '',
    settings INTEGER NOT NULL DEFAULT 0
);




CREATE TABLE IF NOT EXISTS group_messages (
    id BIGINT NOT NULL DEFAULT now_us(),
    group_id BIGINT NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    msg BYTEA NOT NULL,
    epoch INT NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id, id)
);

CREATE TABLE IF NOT EXISTS group_members (
    address_id BIGINT NOT NULL REFERENCES addresses (id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    last_message_seen BIGINT NOT NULL DEFAULT now_us(),
    epoch INT NOT NULL DEFAULT 0,
    PRIMARY KEY (address_id, group_id)
);


CREATE TYPE group_member_update AS (
    address_id BIGINT,
    group_id BIGINT,
    last_message_seen BIGINT,
    last_epoch INT
);

CREATE TABLE IF NOT EXISTS group_commits (
    id BIGINT NOT NULL DEFAULT now_us(),
    group_id BIGINT NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    epoch INT NOT NULL,
    msg BYTEA NOT NULL,

    PRIMARY KEY (group_id, epoch)
);


CREATE TABLE IF NOT EXISTS group_invites (
  group_id BIGINT NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
  inviter VARCHAR NOT NULL,
  invitee_address BIGINT NOT NULL REFERENCES addresses (id) ON DELETE CASCADE,
  commit_id BIGINT NOT NULL,
  msg BYTEA NOT NULL,
  PRIMARY KEY (invitee_address, group_id)
);


CREATE TYPE key_package AS (
  id INTEGER,
  address BIGINT,
  package BYTEA
);


CREATE TABLE IF NOT EXISTS group_key_packages (
  id SMALLINT NOT NULL,
  address BIGINT NOT NULL REFERENCES addresses (id) ON DELETE CASCADE,
  package BYTEA NOT NULL,
  PRIMARY KEY (address, id)
);


CREATE TABLE IF NOT EXISTS keys (
  kid VARCHAR NOT NULL,
  key BYTEA NOT NULL,
  exp TIMESTAMPTZ NOT NULL
);



CREATE TABLE IF NOT EXISTS pre_key_bundles (
  id INT NOT NULL DEFAULT floor(random() * 32000)::int,
  address BIGINT REFERENCES addresses (id) ON DELETE CASCADE,
  bundle BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS pre_key_bundles_addr ON pre_key_bundles (address);


CREATE TYPE pre_key_bundle AS (
  id INT,
  bundle BYTEA
);

CREATE OR REPLACE FUNCTION add_pre_key_bundles(adder VARCHAR, address_id BIGINT, p_device_id SMALLINT, bundles PRE_KEY_BUNDLE[]) RETURNS VOID AS
$$
DECLARE
existing_key_count INT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM addresses WHERE id = address_id AND username = adder AND device_id = p_device_id) THEN
    RAISE EXCEPTION 'address does not exist';
  END IF;
  SELECT COUNT(1) INTO existing_key_count FROM pre_key_bundles WHERE address = address_id;
  IF existing_key_count + cardinality(bundles) > 64 THEN
    RAISE EXCEPTION 'keys are exceeeding limit 64';
  END IF;
  INSERT INTO pre_key_bundles (address, id, bundle)
  SELECT address_id, id, bundle FROM UNNEST(bundles);
END;
$$ LANGUAGE plpgsql;







CREATE OR REPLACE FUNCTION add_key_packages(adder VARCHAR, address_id BIGINT, packages KEY_PACKAGE[]) RETURNS VOID AS
$$
DECLARE
existing_key_count INT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM addresses WHERE id = address_id AND username = adder) THEN
    RAISE EXCEPTION 'address does not exist';
  END IF;
  SELECT COUNT(1) INTO existing_key_count FROM group_key_packages WHERE address = address_id;
  IF existing_key_count + cardinality(packages) > 64 THEN
    RAISE EXCEPTION 'packages are exceeding limit 64';
  END IF;
  INSERT INTO group_key_packages (address, id, package)
  SELECT address, id, package FROM UNNEST(packages)
  ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;






-- user settings
-- requested 1 0
-- accepted  1 1
-- muted 2
-- blocked 4




-- user1 should always be less than user2
CREATE TABLE IF NOT EXISTS user_conversations (
  user1 VARCHAR NOT NULL COLLATE "C",
  user2 VARCHAR NOT NULL COLLATE "C",


  user1_settings BIGINT NOT NULL DEFAULT 0,
  user2_settings BIGINT NOT NULL DEFAULT 0,


  PRIMARY KEY (user1, user2)
);


CREATE INDEX IF NOT EXISTS user_conversations_user2_idx ON user_conversations (user2);

-- ALTER TABLE user_conversations ADD CONSTRAINT user1_lt_user2 CHECK (user1 < user2);
ALTER TABLE user_conversations ADD CONSTRAINT user1_lt_user2 CHECK (user1 COLLATE "C" < user2 COLLATE "C");



CREATE OR REPLACE FUNCTION upsert_user_conversations(p_user1 VARCHAR, p_user2 VARCHAR, p_user1_settings BIGINT, p_user2_settings BIGINT) RETURNS setof user_conversations AS
$$
BEGIN

  IF p_user1_settings IS NOT NULL THEN
    INSERT INTO user_conversations (user1, user2, user1_settings, user2_settings)
    VALUES (p_user1, p_user2, p_user1_settings, 0)
    ON CONFLICT (user1, user2) DO UPDATE SET user1_settings = p_user1_settings RETURNING user2_settings INTO p_user2_settings;
  ELSIF p_user2_settings IS NOT NULL THEN
    INSERT INTO user_conversations (user1, user2, user1_settings, user2_settings)
    VALUES (p_user1, p_user2, 0, p_user2_settings)
    ON CONFLICT (user1, user2) DO UPDATE SET user2_settings = p_user2_settings RETURNING user1_settings INTO p_user1_settings;
  END IF;

  RETURN QUERY SELECT p_user1, p_user2, p_user1_settings, p_user2_settings;

END;
$$ LANGUAGE plpgsql;



CREATE TABLE IF NOT EXISTS user_messages (
  id BIGINT NOT NULL DEFAULT now_us(),
  from_id BIGINT NOT NULL,
  to_id BIGINT NOT NULL REFERENCES addresses (id) ON DELETE CASCADE,
  payload BYTEA NOT NULL,
  ty SMALLINT NOT NULL,
  hvalue BIGINT NOT NULL DEFAULT 0, -- hash value
  verified BOOLEAN NOT NULL DEFAULT FALSE -- verified by to
);

CREATE INDEX IF NOT EXISTS user_messages_to_idx ON user_messages (to_id, id);


CREATE TYPE MESSAGE_ENTRY AS (
  to_address BIGINT,
  payload BYTEA,
  ty SMALLINT,
  hash_value BIGINT
);

CREATE OR REPLACE FUNCTION insert_user_messages(
    from_id BIGINT,
    message_entries MESSAGE_ENTRY[],
    self_message_entries MESSAGE_ENTRY[],
    from_device_id SMALLINT,
    from_username VARCHAR
)
RETURNS TABLE(msg_id BIGINT, to_address BIGINT, is_self BOOLEAN)
LANGUAGE plpgsql AS $$
DECLARE
    target_username TEXT;
    sender_settings BIGINT;
    receiver_settings BIGINT;
BEGIN
    ------------------------------------------------------------------
    -- validate sender
    ------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM addresses
        WHERE id = from_id
          AND device_id = from_device_id
          AND username = from_username
    ) THEN
        RAISE EXCEPTION 'invalid from_address';
    END IF;

    ------------------------------------------------------------------
    -- validate self messages (only existing addresses)
    ------------------------------------------------------------------
    IF EXISTS (
        SELECT 1
        FROM unnest(self_message_entries) m
        JOIN addresses a ON a.id = m.to_address
        WHERE a.username <> from_username
    ) THEN
        RAISE EXCEPTION 'self sending messages donot refer to self username';
    END IF;

    ------------------------------------------------------------------
    -- resolve target username from existing addresses only
    ------------------------------------------------------------------
    SELECT a.username
    INTO target_username
    FROM unnest(message_entries) m
    JOIN addresses a ON a.id = m.to_address
    LIMIT 1;

    -- if *none* of the target addresses exist, nothing to do
    IF target_username IS NULL THEN
        RETURN;
    END IF;

    ------------------------------------------------------------------
    -- ensure all existing message_entries refer to same username
    ------------------------------------------------------------------
    IF EXISTS (
        SELECT 1
        FROM unnest(message_entries) m
        JOIN addresses a ON a.id = m.to_address
        WHERE a.username <> target_username
    ) THEN
        RAISE EXCEPTION 'all message_entries must refer to same username';
    END IF;

    ------------------------------------------------------------------
    -- conversation lookup
    ------------------------------------------------------------------
    IF from_username COLLATE "C" < target_username COLLATE "C" THEN
        SELECT user1_settings, user2_settings
        INTO sender_settings, receiver_settings
        FROM user_conversations
        WHERE user1 = from_username
          AND user2 = target_username;
    ELSE
        SELECT user2_settings, user1_settings
        INTO sender_settings, receiver_settings
        FROM user_conversations
        WHERE user2 = from_username
          AND user1 = target_username;
    END IF;

    IF (COALESCE(sender_settings, 0) & 4) <> 0 OR (COALESCE(receiver_settings, 0) & 4) <> 0 THEN
        RAISE EXCEPTION 'participant blocked';
    END IF;

    ------------------------------------------------------------------
    -- receiver side: insert + missing (existing addresses only)
    ------------------------------------------------------------------
    RETURN QUERY
    WITH targets AS (
        SELECT id
        FROM addresses
        WHERE username = target_username
    ),
    ins AS (
        INSERT INTO user_messages (from_id, to_id, payload, ty, hvalue)
        SELECT from_id, a.id, m.payload, m.ty, m.hash_value
        FROM unnest(message_entries) m
        JOIN addresses a ON a.id = m.to_address
        RETURNING id, to_id
    )
    SELECT i.id, i.to_id, false FROM ins i
    UNION ALL
    SELECT NULL, t.id, false
    FROM targets t
    LEFT JOIN ins i ON i.to_id = t.id
    WHERE i.id IS NULL;

    ------------------------------------------------------------------
    -- self side: insert + missing (existing addresses only)
    ------------------------------------------------------------------
    RETURN QUERY
    WITH targets AS (
        SELECT id
        FROM addresses
        WHERE username = from_username
          AND id <> from_id
    ),
    ins AS (
        INSERT INTO user_messages (from_id, to_id, payload, ty)
        SELECT from_id, a.id, m.payload, m.ty
        FROM unnest(self_message_entries) m
        JOIN addresses a ON a.id = m.to_address
        RETURNING id, to_id
    )
    SELECT i.id, i.to_id, true FROM ins i
    UNION ALL
    SELECT NULL, t.id, true
    FROM targets t
    LEFT JOIN ins i ON i.to_id = t.id
    WHERE i.id IS NULL;

END;
$$;




CREATE OR REPLACE FUNCTION get_messages_for_user(
    target_id BIGINT,
    expected_username VARCHAR,
    since BIGINT,
    lim INT
)
RETURNS TABLE(
  id BIGINT,
  from_id BIGINT,
  from_username VARCHAR,
  from_device_id SMALLINT,
  payload BYTEA,
  ty SMALLINT
)
LANGUAGE plpgsql AS $$
BEGIN
    -- Check ownership
    IF NOT EXISTS (
        SELECT 1 FROM addresses a
        WHERE a.id = target_id AND username = expected_username
    ) THEN
        RAISE EXCEPTION 'unauthorized';
    END IF;

    RETURN QUERY
    SELECT m.id, m.from_id, a.username AS from_username, a.device_id AS from_device_id, m.payload, m.ty
    FROM user_messages m
    JOIN addresses a ON a.id = m.from_id
    WHERE m.to_id = target_id AND m.id > since
    ORDER BY m.id
    LIMIT lim;
END;
$$;


CREATE TYPE group_commit_sync_request AS (
    group_id BIGINT,
    epoch INT
);


CREATE TYPE group_sync_request AS (
    group_id BIGINT,
    start_after BIGINT
);

CREATE OR REPLACE FUNCTION get_all_group_messages(
    p_address_id BIGINT,
    p_requests group_sync_request[],
    p_max_limit INT
)
RETURNS SETOF group_messages AS $$
BEGIN
RETURN QUERY
    WITH req AS (
        SELECT
            r.group_id,
            r.start_after
        FROM unnest(p_requests) r
    )
    SELECT gm.*
    FROM group_messages gm
    JOIN req r
      ON r.group_id = gm.group_id
    JOIN group_members m
      ON m.group_id = gm.group_id
     AND m.address_id = p_address_id
    WHERE
        gm.id > r.start_after
    ORDER BY gm.id
    LIMIT p_max_limit;
END;
$$ LANGUAGE plpgsql STABLE;



CREATE TABLE IF NOT EXISTS group_re_add_requests(
    group_id BIGINT REFERENCES groups (id) ON DELETE CASCADE NOT NULL,
    address_id BIGINT REFERENCES addresses (id) ON DELETE CASCADE NOT NULL,

    PRIMARY KEY (group_id, address_id)
);


-- migrations #1
ALTER TABLE addresses ADD COLUMN last_connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW();


-- migrations #2
CREATE TABLE IF NOT EXISTS group_join_links (
    token VARCHAR PRIMARY KEY,
    group_id BIGINT REFERENCES groups(id) ON DELETE CASCADE,
    created_by VARCHAR,
    expires_at TIMESTAMPTZ,
    max_uses INT,
    current_uses INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS group_join_requests (
    group_id BIGINT REFERENCES groups(id) ON DELETE CASCADE,
    address_id BIGINT REFERENCES addresses(id),
    username VARCHAR,
    token VARCHAR REFERENCES group_join_links(token) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (group_id, address_id)
);

-- migrations #3
CREATE TABLE IF NOT EXISTS group_member_history (
    id BIGSERIAL PRIMARY KEY,
    group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    address_id BIGINT NOT NULL REFERENCES addresses(id) ON DELETE CASCADE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    left_at TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION on_group_member_change() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO group_member_history (group_id, address_id, joined_at)
        VALUES (NEW.group_id, NEW.address_id, NOW());
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE group_member_history
        SET left_at = NOW()
        WHERE group_id = OLD.group_id
          AND address_id = OLD.address_id
          AND left_at IS NULL;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER group_members_history_trigger
    AFTER INSERT OR DELETE ON group_members
    FOR EACH ROW EXECUTE FUNCTION on_group_member_change();




-- Migration to add upgraded column to groups table
ALTER TABLE groups ADD COLUMN IF NOT EXISTS upgraded BOOLEAN NOT NULL DEFAULT FALSE;




-- Migration: Group Meeting Sessions
CREATE TABLE IF NOT EXISTS group_meeting_sessions (
    session_id BIGINT NOT NULL DEFAULT now_us() PRIMARY KEY,
    group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    channel_id INT NOT NULL,
    creator_username VARCHAR NOT NULL,
    status SMALLINT NOT NULL DEFAULT 0,  -- 0 = active, 1 = ended
    cf_meeting_id VARCHAR NOT NULL,
    e2ee_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at BIGINT NOT NULL DEFAULT now_us(),
    ended_at BIGINT
);

-- Index for fast lookup of active sessions per channel
CREATE INDEX IF NOT EXISTS idx_meeting_sessions_active
    ON group_meeting_sessions (group_id, channel_id, status)
    WHERE status = 0;

CREATE TABLE IF NOT EXISTS group_meeting_participants (
    session_id BIGINT NOT NULL REFERENCES group_meeting_sessions(session_id) ON DELETE CASCADE,
    username VARCHAR NOT NULL,
    joined_at BIGINT NOT NULL DEFAULT now_us(),
    left_at BIGINT,
    PRIMARY KEY (session_id, username)
);

-- # migrations
-- Drop foreign key constraint on user_messages.from_id and check valid address on from_id
CREATE OR REPLACE FUNCTION check_address_exists(addr_id BIGINT) RETURNS BOOLEAN AS $$
    SELECT EXISTS (SELECT 1 FROM addresses WHERE id = addr_id);
$$ LANGUAGE sql IMMUTABLE;

ALTER TABLE user_messages DROP CONSTRAINT IF EXISTS user_messages_from_id_fkey;
ALTER TABLE user_messages ADD CONSTRAINT check_from_id_exists CHECK (check_address_exists(from_id));





