-- DuckDB test environment for sql_meta
-- Mirrors the MSSQL mssql_testenv.sql schema/data using DuckDB syntax.

DROP TABLE IF EXISTS deaths;
DROP TABLE IF EXISTS sites;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS cause_categories;
DROP TABLE IF EXISTS causes;

CREATE TABLE causes (
  code INTEGER NOT NULL,
  label VARCHAR NOT NULL,
  description TEXT NULL,
  PRIMARY KEY (code)
);
COMMENT ON COLUMN causes.code IS 'Unique code for each cause';

CREATE TABLE cause_categories (
  cause_category INTEGER NOT NULL,
  label VARCHAR NOT NULL,
  PRIMARY KEY (cause_category)
);
COMMENT ON COLUMN cause_categories.cause_category IS 'High-level cause grouping without FK references';

CREATE TABLE sources (
  id INTEGER NOT NULL,
  name VARCHAR NOT NULL,
  PRIMARY KEY (id)
);
COMMENT ON COLUMN sources.id IS 'Unique identifier for each source';

CREATE TABLE sites (
  site_id INTEGER NOT NULL,
  source_id INTEGER NULL,
  PRIMARY KEY (site_id),
  FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE deaths (
  death_id INTEGER NOT NULL,
  site_id INTEGER NOT NULL,
  cause INTEGER NULL,
  cause_category INTEGER NULL,
  PRIMARY KEY (death_id),
  FOREIGN KEY (site_id) REFERENCES sites(site_id),
  FOREIGN KEY (cause) REFERENCES causes(code)
);
COMMENT ON COLUMN deaths.cause IS 'The cause of the death';

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
