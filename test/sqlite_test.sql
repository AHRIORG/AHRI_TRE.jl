-- tre.causes definition

CREATE TABLE causes (
  code INTEGER NOT NULL,
  label TEXT NOT NULL,
  description TEXT DEFAULT NULL,
  PRIMARY KEY (code)
);

-- tre.deaths definition

CREATE TABLE deaths (
  death_id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  cause INTEGER DEFAULT NULL
);

CREATE INDEX IX_deaths_site_id ON deaths (site_id);
CREATE INDEX IX_deaths_cause ON deaths (cause);

-- tre.sites definition

CREATE TABLE sites (
  site_id INTEGER NOT NULL,
  source_id INTEGER DEFAULT NULL,
  PRIMARY KEY (site_id)
);

CREATE INDEX IX_sites_source_id ON sites (source_id);

-- tre.sources definition

CREATE TABLE sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL
);

-- Foreign key constraints (SQLite supports these but foreign key enforcement must be enabled)
-- PRAGMA foreign_keys = ON;

-- Note: SQLite doesn't support ALTER TABLE ADD CONSTRAINT for foreign keys
-- Foreign key constraints must be defined inline during table creation
-- For this conversion, we'll recreate the tables with foreign keys

DROP TABLE IF EXISTS deaths;
DROP TABLE IF EXISTS sites;

-- Recreate sites table with foreign key
CREATE TABLE sites (
  site_id INTEGER NOT NULL,
  source_id INTEGER DEFAULT NULL,
  PRIMARY KEY (site_id),
  FOREIGN KEY (source_id) REFERENCES sources (id)
);

CREATE INDEX IX_sites_source_id ON sites (source_id);

-- Recreate deaths table with foreign keys
CREATE TABLE deaths (
  death_id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  cause INTEGER DEFAULT NULL,
  FOREIGN KEY (site_id) REFERENCES sites (site_id),
  FOREIGN KEY (cause) REFERENCES causes (code)
);

CREATE INDEX IX_deaths_site_id ON deaths (site_id);
CREATE INDEX IX_deaths_cause ON deaths (cause);

-- Insert data
INSERT INTO causes (code, label, description) VALUES
  (1, 'Natural', 'Died from natural causes'),
  (2, 'Unnatural', 'Did not die from natural causes');

INSERT INTO sources (name) VALUES
  ('Alpha'),
  ('Beta');

INSERT INTO sites (site_id, source_id) VALUES
  (10, 1),
  (20, 2);

-- SQLite AUTOINCREMENT will handle death_id automatically
INSERT INTO deaths (site_id, cause) VALUES
  (10, 1),
  (10, 1),
  (10, 1),
  (10, 1),
  (10, 2),
  (10, 2),
  (10, 1),
  (10, 2),
  (10, 1),
  (20, 1),
  (20, 1),
  (20, 1),
  (20, 1),
  (20, 1),
  (20, 2),
  (20, 1),
  (20, 1),
  (20, 1),
  (20, 1);