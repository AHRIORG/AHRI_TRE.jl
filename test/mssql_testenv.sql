-- causes definition

CREATE TABLE [causes] (
  [code] int NOT NULL,
  [label] nvarchar(255) NOT NULL,
  [description] ntext NULL,
  CONSTRAINT [PK_causes] PRIMARY KEY ([code])
);

-- Add comments using extended properties
EXEC sys.sp_addextendedproperty 
  @name = N'MS_Description', 
  @value = N'Unique code for each cause', 
  @level0type = N'SCHEMA', @level0name = N'dbo', 
  @level1type = N'TABLE', @level1name = N'causes', 
  @level2type = N'COLUMN', @level2name = N'code';

-- deaths definition

CREATE TABLE [deaths] (
  [death_id] int IDENTITY(1,1) NOT NULL,
  [site_id] int NOT NULL,
  [cause] int NULL,
  CONSTRAINT [PK_deaths] PRIMARY KEY ([death_id])
);

CREATE INDEX [IX_deaths_site_id] ON [deaths] ([site_id]);
CREATE INDEX [IX_deaths_cause] ON [deaths] ([cause]);

-- Add comment for cause column
EXEC sys.sp_addextendedproperty 
  @name = N'MS_Description', 
  @value = N'The cause of the death', 
  @level0type = N'SCHEMA', @level0name = N'dbo', 
  @level1type = N'TABLE', @level1name = N'deaths', 
  @level2type = N'COLUMN', @level2name = N'cause';

-- sites definition

CREATE TABLE [sites] (
  [site_id] int NOT NULL,
  [source_id] int NULL,
  CONSTRAINT [PK_sites] PRIMARY KEY ([site_id])
);

CREATE INDEX [IX_sites_source_id] ON [sites] ([source_id]);

-- sources definition

CREATE TABLE [sources] (
  [id] int IDENTITY(1,1) NOT NULL,
  [name] nvarchar(100) NOT NULL,
  CONSTRAINT [PK_sources] PRIMARY KEY ([id])
);

-- Add comment for id column
EXEC sys.sp_addextendedproperty 
  @name = N'MS_Description', 
  @value = N'Unique identifier for each source', 
  @level0type = N'SCHEMA', @level0name = N'dbo', 
  @level1type = N'TABLE', @level1name = N'sources', 
  @level2type = N'COLUMN', @level2name = N'id';

-- Foreign key constraints
ALTER TABLE [deaths] ADD CONSTRAINT [FK_deaths_sites] 
  FOREIGN KEY ([site_id]) REFERENCES [sites] ([site_id]);

ALTER TABLE [deaths] ADD CONSTRAINT [FK_deaths_causes] 
  FOREIGN KEY ([cause]) REFERENCES [causes] ([code]);

ALTER TABLE [sites] ADD CONSTRAINT [FK_sites_sources] 
  FOREIGN KEY ([source_id]) REFERENCES [sources] ([id]);

INSERT INTO causes (code,label,description) VALUES
	 (1,'Natural','Died from natural causes'),
	 (2,'Unnatural','Did not die from natural causes');

INSERT INTO sources (name) VALUES
	 ('Alpha'),
	 ('Beta');

INSERT INTO sites (site_id,source_id) VALUES
	 (10,1),
	 (20,2);

INSERT INTO deaths (site_id,cause) VALUES
	 (10,1),
	 (10,1),
	 (10,1),
	 (10,1),
	 (10,2),
	 (10,2),
	 (10,1),
	 (10,2),
	 (10,1),
	 (20,1);
INSERT INTO deaths (site_id,cause) VALUES
	 (20,1),
	 (20,1),
	 (20,1),
	 (20,1),
	 (20,2),
	 (20,1),
	 (20,1),
	 (20,1),
	 (20,1);
