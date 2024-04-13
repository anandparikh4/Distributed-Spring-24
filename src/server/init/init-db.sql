-- PostgreSQL 16.2
BEGIN TRANSACTION;

-- Create the table for the term
CREATE TABLE IF NOT EXISTS TermT (
	shard_id TEXT PRIMARY KEY,
	last_idx INTEGER NOT NULL DEFAULT 0
	executed BOOLEAN NOT NULL DEFAULT TRUE
);

-- Create the table for the student
CREATE TABLE IF NOT EXISTS StudT (
	stud_id INTEGER PRIMARY KEY,
	stud_name TEXT NOT NULL,
	stud_marks INTEGER NOT NULL,
	shard_id TEXT NOT NULL,
	FOREIGN KEY (shard_id) REFERENCES TermT (shard_id)
);

-- Create the table for the log
CREATE TABLE IF NOT EXISTS LogT (
	log_idx INTEGER NOT NULL,
	shard_id TEXT NOT NULL,
	operation TEXT NOT NULL,
	stud_id INTEGER DEFAULT NULL,
	content JSON DEFAULT NULL,
	PRIMARY KEY (log_idx,shard_id)
	FOREIGN KEY (shard_id) REFERENCES TermT (shard_id)
);

-- Create the index for the student
CREATE INDEX IF NOT EXISTS idx_studt_shard_id ON StudT (shard_id);

-- Create the index for the log
CREATE INDEX IF NOT EXISTS idx_logt_shard_id ON LogT (shard_id);

COMMIT TRANSACTION;
