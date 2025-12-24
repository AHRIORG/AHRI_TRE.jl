-- tre.causes definition

CREATE TABLE causes (
  code integer NOT NULL,
  label varchar(255) NOT NULL,
  description text DEFAULT NULL
);

COMMENT ON COLUMN causes.code IS 'Unique code for each cause';
ALTER TABLE causes ADD CONSTRAINT PK_causes PRIMARY KEY (code);

CREATE TABLE cause_categories (
  cause_category integer NOT NULL,
  label varchar(255) NOT NULL
);

COMMENT ON COLUMN cause_categories.cause_category IS 'High-level cause grouping without FK constraint';
ALTER TABLE cause_categories ADD CONSTRAINT PK_cause_categories PRIMARY KEY (cause_category);

-- tre.deaths definition

CREATE TABLE deaths (
  death_id serial NOT NULL,
  site_id integer NOT NULL,
  cause integer DEFAULT NULL,
  cause_category integer DEFAULT NULL
);

COMMENT ON COLUMN deaths.cause IS 'The cause of the death';
ALTER TABLE deaths ADD CONSTRAINT PK_deaths PRIMARY KEY (death_id);
CREATE INDEX IX_deaths_site_id ON deaths (site_id);
CREATE INDEX IX_deaths_cause ON deaths (cause);

-- tre.sites definition

CREATE TABLE sites (
  site_id integer NOT NULL,
  source_id integer DEFAULT NULL
);

ALTER TABLE sites ADD CONSTRAINT PK_sites PRIMARY KEY (site_id);
CREATE INDEX IX_sites_source_id ON sites (source_id);

-- tre.sources definition

CREATE TABLE sources (
  id serial NOT NULL,
  name varchar(100) NOT NULL
);

COMMENT ON COLUMN sources.id IS 'Unique identifier for each source';
ALTER TABLE sources ADD CONSTRAINT PK_sources PRIMARY KEY (id);

-- Foreign key constraints
ALTER TABLE deaths ADD CONSTRAINT FK_deaths_sites 
  FOREIGN KEY (site_id) REFERENCES sites (site_id);

ALTER TABLE deaths ADD CONSTRAINT FK_deaths_causes 
  FOREIGN KEY (cause) REFERENCES causes (code);

ALTER TABLE sites ADD CONSTRAINT FK_sites_sources 
  FOREIGN KEY (source_id) REFERENCES sources (id);

-- Insert data
INSERT INTO causes (code, label, description) VALUES
  (1, 'Natural', 'Died from natural causes'),
  (2, 'Unnatural', 'Did not die from natural causes');

INSERT INTO cause_categories (cause_category, label) VALUES
  (1, 'Medical'),
  (2, 'External');

INSERT INTO sources (name) VALUES
  ('Alpha'),
  ('Beta');

INSERT INTO sites (site_id, source_id) VALUES
  (10, 1),
  (20, 2);

INSERT INTO deaths (site_id, cause, cause_category) VALUES
  (10, 1, 1),
  (10, 1, 1),
  (10, 1, 1),
  (10, 1, 1),
  (10, 2, 2),
  (10, 2, 2),
  (10, 1, 1),
  (10, 2, 2),
  (10, 1, 1),
  (20, 1, 1),
  (20, 1, 1),
  (20, 1, 1),
  (20, 1, 1),
  (20, 1, 1),
  (20, 2, 2),
  (20, 1, 1),
  (20, 1, 1),
  (20, 1, 1),
  (20, 1, 1);