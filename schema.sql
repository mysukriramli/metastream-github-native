-- Core MetaStream Bitemporal Schema for DuckDB
CREATE SEQUENCE IF NOT EXISTS seq_commit_id START 1;
CREATE SEQUENCE IF NOT EXISTS seq_change_id START 1;

CREATE TABLE IF NOT EXISTS entities (
    entity_id VARCHAR PRIMARY KEY,
    entity_type VARCHAR,
    canonical_label VARCHAR
);

CREATE TABLE IF NOT EXISTS evidence (
    evidence_id VARCHAR PRIMARY KEY,
    source_id VARCHAR,
    retrieved_at TIMESTAMP,
    artifact_hash VARCHAR,
    release_url VARCHAR
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id VARCHAR PRIMARY KEY,
    entity_id VARCHAR,
    predicate VARCHAR,
    object_value JSON,
    source_id VARCHAR,
    evidence_id VARCHAR,
    valid_from TIMESTAMP,
    first_observed_at TIMESTAMP,
    status VARCHAR DEFAULT 'ACTIVE',
    content_hash VARCHAR
);

CREATE TABLE IF NOT EXISTS changes (
    change_id BIGINT DEFAULT nextval('seq_change_id'),
    commit_id BIGINT,
    entity_id VARCHAR,
    operation VARCHAR,
    claim_id VARCHAR,
    timestamp_utc TIMESTAMP
);
