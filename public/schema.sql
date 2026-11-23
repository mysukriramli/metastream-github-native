CREATE SEQUENCE seq_change_id INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 2 NO CYCLE;
CREATE SEQUENCE seq_commit_id INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 2 NO CYCLE;
CREATE TABLE changes(change_id BIGINT DEFAULT(nextval('seq_change_id')), commit_id BIGINT, entity_id VARCHAR, operation VARCHAR, claim_id VARCHAR, timestamp_utc TIMESTAMP);
CREATE TABLE claims(claim_id VARCHAR PRIMARY KEY, entity_id VARCHAR, predicate VARCHAR, object_value JSON, source_id VARCHAR, evidence_id VARCHAR, valid_from TIMESTAMP, first_observed_at TIMESTAMP, status VARCHAR DEFAULT('ACTIVE'), content_hash VARCHAR);
CREATE TABLE entities(entity_id VARCHAR PRIMARY KEY, entity_type VARCHAR, canonical_label VARCHAR);
CREATE TABLE evidence(evidence_id VARCHAR PRIMARY KEY, source_id VARCHAR, retrieved_at TIMESTAMP, artifact_hash VARCHAR, release_url VARCHAR);

