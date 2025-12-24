-- SQLite test environment for sql_meta
-- Mirrors the MSSQL/DuckDB test schema using SQLite syntax.

PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS deaths;
DROP TABLE IF EXISTS sites;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS cause_categories;
DROP TABLE IF EXISTS causes;

CREATE TABLE causes (
  code INTEGER NOT NULL PRIMARY KEY,
  label TEXT NOT NULL,
  description TEXT
);

CREATE TABLE cause_categories (
  cause_category INTEGER NOT NULL PRIMARY KEY,
  label TEXT NOT NULL
);

CREATE TABLE sources (
  id INTEGER NOT NULL PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE sites (
  site_id INTEGER NOT NULL PRIMARY KEY,
  source_id INTEGER,
  FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE deaths (
  death_id INTEGER NOT NULL PRIMARY KEY,
  site_id INTEGER NOT NULL,
  cause INTEGER,
  cause_category INTEGER,
  FOREIGN KEY (site_id) REFERENCES sites(site_id),
  FOREIGN KEY (cause) REFERENCES causes(code)
);

CREATE INDEX ix_deaths_site_id ON deaths(site_id);
CREATE INDEX ix_deaths_cause ON deaths(cause);
CREATE INDEX ix_sites_source_id ON sites(source_id);

INSERT INTO causes (code, label, description) VALUES
  (1, 'Natural', 'Died from natural causes'),
  (2, 'Unnatural', 'Did not die from natural causes');

INSERT INTO cause_categories (cause_category, label) VALUES
  (1, 'Medical'),
  (2, 'External');

INSERT INTO sources (id, name) VALUES
  (1, 'Alpha'),
  (2, 'Beta');

INSERT INTO sites (site_id, source_id) VALUES
  (10, 1),
  (20, 2);

INSERT INTO deaths (death_id, site_id, cause, cause_category) VALUES
  (1, 10, 1, 1),
  (2, 10, 1, 1),
  (3, 10, 1, 1),
  (4, 10, 1, 1),
  (5, 10, 2, 2),
  (6, 10, 2, 2),
  (7, 10, 1, 1),
  (8, 10, 2, 2),
  (9, 10, 1, 1),
  (10, 20, 1, 1),
  (11, 20, 1, 1),
  (12, 20, 1, 1),
  (13, 20, 1, 1),
  (14, 20, 1, 1),
  (15, 20, 2, 2),
  (16, 20, 1, 1),
  (17, 20, 1, 1),
  (18, 20, 1, 1),
  (19, 20, 1, 1);
