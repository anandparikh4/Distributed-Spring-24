-- PostgreSQL 16.2
BEGIN TRANSACTION;

-- Create the table for the term
CREATE TABLE IF NOT EXISTS TermT (
	shard_id TEXT PRIMARY KEY,
	term INTEGER NOT NULL DEFAULT 0
);

-- Create the table for the student
CREATE TABLE IF NOT EXISTS StudT (
	stud_id INTEGER NOT NULL,
	stud_name TEXT NOT NULL,
	stud_marks INTEGER NOT NULL,
	shard_id TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	deleted_at INTEGER DEFAULT NULL,
	PRIMARY KEY (stud_id, created_at) INCLUDE (deleted_at),
	FOREIGN KEY (shard_id) REFERENCES TermT (shard_id)
);

-- Create the index for the student
CREATE INDEX IF NOT EXISTS idx_studt_shard_id ON StudT (shard_id);

COMMIT TRANSACTION;


