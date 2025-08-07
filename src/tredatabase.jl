"""
    createdatabase(path, name; replace=false, sqlite=true)

Creates a database to store the information contained in the Trusted Research Environment (TRE)
By default a sqlite database is created, but this can be changed by setting the sqlite argument to false, 
in which case a sql server database is created and path is interpreted as the name of the database server.
"""
function createdatabase(server, user, password, database; replace=false)
    conn = DBInterface.connect(MySQL.Connection, server, user, password)
    if replace
        sql = "DROP DATABASE IF EXISTS $database;"
        DBInterface.execute(conn, sql)
    end
    sql = "CREATE DATABASE IF NOT EXISTS $database;"
    DBInterface.execute(conn, sql)
    sql = "USE $database;"
    DBInterface.execute(conn, sql)
    try
        createstudies(conn)
        createtransformations(conn)
        createvariables(conn)
        createassets(conn)
        createentities(conn)
        createmapping(conn)
        return nothing
    finally
        DBInterface.close!(conn)
    end
end
function opendatastore(store::DataStore)::DataStore
    conn, lake = opendatastore(store.server, store.user, store.password, store.dbname, store.lake_data, store.lake_db, 
        store.lake_user, store.lake_password)
    store.store = conn
    store.lake = lake
    return store
end
"""
    opendatastore(server::AbstractString, user::AbstractString, password::AbstractString, database::AbstractString, lake_data::Union{String,Nothing}=nothing, lake_db::Union{String,Nothing}=nothing)
Open a database connection to a MySQL server with optional DuckDB data lake support.
This function connects to a MySQL server using the provided credentials and database name.
"""
function opendatastore(server::AbstractString, user::AbstractString, password::AbstractString, database::AbstractString,
    lake_data::Union{String,Nothing}=nothing, lake_db::Union{String,Nothing}=nothing, 
    lake_user::Union{String,Nothing}=nothing, lake_password::Union{String,Nothing}=nothing)
    conn = DBInterface.connect(MySQL.Connection, server, user, password; db = database) #, unix_socket="/var/run/mysqld/mysqld.sock"
    @info "Connected to database $(database) on server $(server)"
    # Open ducklake database if lake_data and lake_db are provided
    # ATTACH 'ducklake:mysql:host=localhost port=3306 user=ducklake_user password=Nzy-f6y@brNF_6AFaC2MrZAU database=ducklake_catalog' AS my_ducklake (DATA_PATH '/data/datalake', METADATA_SCHEMA 'ducklake_catalog');
    lake = nothing
    if !isnothing(lake_data) && !isnothing(lake_db)
        @info "Opening DuckDB data lake at $(lake_data) with metadata database $lake_db"
        # Ensure the lake_data directory exists
        if !isdir(lake_data)
            mkpath(lake_data)
        end
        ddb = DuckDB.DB()
        lake = DBInterface.connect(ddb)
        df = DuckDB.query(ddb, "SELECT version() as duckdb_version;") |> DataFrame
        @info df
        DBInterface.execute(lake, "LOAD 'ducklake';")
        DBInterface.execute(lake, "LOAD 'mysql';")
        df = DuckDB.query(ddb, "UPDATE EXTENSIONS;") |> DataFrame
        @info df

        # Attach the data lake database
        DBInterface.execute(lake, "ATTACH 'ducklake:mysql:host=$(server) port=3306 user=$(lake_user) password=$(lake_password) database=$(lake_db)' AS rda_lake 
        (DATA_PATH '$lake_data', METADATA_SCHEMA 'ducklake_catalog');")
        DBInterface.execute(lake, "USE rda_lake;")
        @info "Attached DuckDB data lake at $(lake_data) with metadata database $lake_db"
    end
    return conn, lake
end
"""
    closedatastore(store::DataStore)

Close the connections in a DataStore object
"""
function closedatastore(store::DataStore)
    DBInterface.close!(store.store) 
    if !isnothing(store.lake)
        DBInterface.close!(store.lake)
    end
    @info "Closed datastore connections"
    return nothing
end
"""
    get_table(conn::MySQL.Connection, table::String)::AbstractDataFrame

Retrieve table `table` as a DataFrame from `conn`
"""
function get_table(conn::MySQL.Connection, table::String)::AbstractDataFrame
    sql = "SELECT * FROM $(table)"
    df = DBInterface.execute(conn, sql) |> DataFrame
    return df
end
"""
    makeparam(s)

Return a parameterized string for SQL queries, e.g., "?" for MySQL
"""
makeparam(s) = "?"
"""
    savedataframe(con::MySQL.Connection, df::AbstractDataFrame, table)

Save a DataFrame to a database table, the names of the dataframe columns should be identical to the table column names in the database
"""
function savedataframe(con::MySQL.Connection, df::AbstractDataFrame, table)
    colnames = names(df)
    paramnames = map(makeparam, colnames) #add @ to column names
    sql = "INSERT INTO $table ($(join(colnames, ", "))) VALUES ($(join(paramnames, ", ")));"
    stmt = DBInterface.prepare(con, sql)
    for row in eachrow(df)
        DBInterface.execute(stmt, NamedTuple(row))
    end
end
"""
    prepareinsertstatement(conn::MySQL.Connection, table, columns)

Prepare an insert statement for MySQL into table for columns
"""
function prepareinsertstatement(conn::MySQL.Connection, table, columns)
    paramnames = map(makeparam, columns) # '?' for MySQL
    sql = "INSERT INTO $table ($(join(columns, ", "))) VALUES ($(join(paramnames, ", ")));"
    return DBInterface.prepare(conn, sql)
end

"""
    updatevalue(conn::MySQL.Connection, table, condition_column, column, condition_value, value)

Update value of column given condition_value in condition_column
"""
function updatevalues(conn::MySQL.Connection, table, condition_column, condition_value, columns, values)
    sql = """
        UPDATE $table 
        SET $(join([col * " = ?" for col in columns], ", "))
        WHERE $condition_column = ?
        """
    stmt = DBInterface.prepare(conn, sql)
    DBInterface.execute(stmt, vcat(values, condition_value))
    return nothing
end
"""
    insertwithidentity(conn::MySQL.Connection, table, columns, values, keycol)

Insert a record, returning the identity column value
"""
function insertwithidentity(conn::MySQL.Connection, table, columns, values)
    paramnames = map(makeparam, columns)
    sql = """
    INSERT INTO $table ($(join(columns, ", "))) 
    VALUES ($(join(paramnames, ", ")));
    """
    stmt = DBInterface.prepare(conn, sql)
    return DBInterface.lastrowid(DBInterface.execute(stmt, values))
end

"""
    insertdata(conn::MySQL.Connection, table, columns, values)

Insert a set of values into a table, columns list the names of the columns to insert, and values the values to insert
"""
function insertdata(conn::MySQL.Connection, table, columns, values)
    stmt = prepareinsertstatement(conn, table, columns)
    return DBInterface.execute(stmt, values)
end

"""
    prepareselectstatement(conn::MySQL.Connection, table, columns::Vector{String}, filter::Vector{String})

Return a statement to select columns from a table, with 0 to n columns to filter on
"""
function prepareselectstatement(conn::MySQL.Connection, table, columns::Vector{String}, filter::Vector{String})
    # Start with the SELECT clause
    select_clause = "SELECT " * join(columns, ", ") * " FROM " * table

    # Check if there are any filter conditions and build the WHERE clause
    if isempty(filter)
        return DBInterface.prepare(conn, select_clause)
    else
        where_clause = " WHERE " * join(["$col = ?" for col in filter], " AND ")
        return DBInterface.prepare(conn, select_clause * where_clause)
    end
end
"""
    selectdataframe(conn::MySQL.Connection, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame

Return a dataframe from a table, with 0 to n columns to filter on
"""
function selectdataframe(conn::MySQL.Connection, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame
    stmt = prepareselectstatement(conn, table, columns, filter)
    return DBInterface.execute(stmt, filtervalues) |> DataFrame
end
"""
    createstudies(conn::MySQL.Connection)

Creates tables to record a study and associated site/s for deaths contributed to the TRE
"""
function createstudies(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `study_types` (
    `study_type_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `name` VARCHAR(80) NOT NULL,
    `namespace` VARCHAR(255) NULL COMMENT 'Namespace of the study type, using http://purl.obolibrary.org/obo/OBI_0500000',
    `ontology_class` VARCHAR(80) NULL COMMENT 'Class identifier of the study type, e.g. EFO_0000408',
    `description` TEXT
    ) COMMENT = 'Study types table to record different types of studies contributing data to the TRE';
    """
    DBInterface.execute(conn, sql)
    @info "Created study_types table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `studies` (
    `study_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `name` varchar(128) NOT NULL UNIQUE COMMENT 'Name of the study, must be unique',
    `description` TEXT,
    `external_id` VARCHAR(128) NULL COMMENT 'External identifier for the study, e.g. from a registry or sponsor',
    `study_type_id` INTEGER COMMENT 'Type of study, e.g. HDSS, Cohort, Survey, etc.',
    `date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `created_by` VARCHAR(255) DEFAULT (CURRENT_USER()) COMMENT 'User who created the study record',
    CONSTRAINT `fk_sources_study_type_id` FOREIGN KEY (`study_type_id`) REFERENCES `study_types` (`study_type_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    ) COMMENT = 'Studies table to record information about studies contributing data to the TRE';
    """
    DBInterface.execute(conn, sql)
    @info "Created studies table"
    DBInterface.execute(conn, initstudytypes())
    @info "Initialized study types"
    return nothing
end

"""
    initstudytypes()

Default transformation types
"""
initstudytypes() = """
    -- Insert the values
    INSERT INTO study_types (study_type_id, name, description, namespace, ontology_class) VALUES
    (1,  'HDSS', 'Health and Demographic Surveillance System','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','population_surveillance'),
    (2,  'COHORT', 'Cohort Study', 'http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl', 'cohort_study'),
    (3,  'SURVEY', 'Cross-sectional Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','cross_sectional_study'),
    (4,  'PANEL', 'Longitudinal/Panel Survey','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','panel_study'),
    (5,  'CASE_CONTROL', 'Case-Control Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','case_control_design'),
    (6,  'RCT', 'Randomized Controlled Trial','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','randomized_controlled_trial'),

    -- Quantitative
    (7,  'QUASI_EXPERIMENTAL', 'Quasi-experimental Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','quasi_experimental_design'),
    (8,  'NATURAL_EXPERIMENT', 'Natural Experiment','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','natural_experiment'),
    (10, 'LAB_EXPERIMENT', 'Laboratory Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','laboratory_study'),

    -- Qualitative
    (11, 'QUALITATIVE_INTERVIEW', 'In-depth or Key Informant Interviews','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','interview_study'),
    (12, 'FOCUS_GROUP', 'Focus Group Discussion','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','focus_group'),
    (13, 'ETHNOGRAPHY', 'Ethnographic Study', 'http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','ethnographic_study'),
    (14, 'PARTICIPATORY', 'Participatory Action Research','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','community_based_participatory_study'),
    (15, 'CASE_STUDY', 'Case Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','case_study'),

    -- Mixed methods
    (16, 'MIXED_METHODS', 'Mixed Methods Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','mixed_method_evaluation'),

    -- Secondary / Desk Review
    (17, 'SECONDARY_ANALYSIS', 'Secondary Data Analysis','http://purl.bioontology.org/ontology/MESH','D000094422'),
    (18, 'DESK_REVIEW', 'Desk or Literature Review','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','literature_review'),

    -- Social / Behavioural
    (19, 'TIME_MOTION', 'Time and Motion Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','time_and_motion_study'),
    (20, 'DIARY', 'Diary Study','http://purl.bioontology.org/ontology/CSP','4009-0001'),
    (21, 'LONGITUDINAL_OBSERVATION', 'Longitudinal Observational Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','longitudinal_study'),

    -- Simulation / Modelling
    (22, 'SIMULATION', 'Simulation Study','http://edamontology.org','data_3869'),
    (23, 'AGENT_BASED_MODEL', 'Agent-based Modelling','https://i2insights.org/index/integration-and-implementation-sciences-vocabulary','agent-based-modelling'),
    (24, 'STATISTICAL_MODEL', 'Statistical Modelling','http://edamontology.org','operation_3664'),
    (25, 'SYSTEM_DYNAMICS', 'Biological system modelling','http://edamontology.org','topic_3075'),

    -- Genomics / Biomedical
    (26, 'GENOMICS', 'Genomics Study','http://ontologies.dbmi.pitt.edu/edda/StudyDesigns.owl','genetic_study'),
    (27, 'MULTIOMICS', 'Multi-omics Study (e.g., proteomics, metabolomics)','http://purl.bioontology.org/ontology/MESH','D000095028'),
    (28, 'BIOBANK', 'Biobank-based Study','http://purl.obolibrary.org/obo','OBIB_0000616'),
    (29, 'PHARMACOGENOMICS', 'Pharmacogenomics Study','http://edamontology.org','topic_0208');
"""
"""
    createtransformations(conn)

Create tables to record data transformations and data ingests
"""
function createtransformations(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformations` (
    `transformation_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `transformation_type` ENUM('ingest','transform','entity') NOT NULL COMMENT 'Type of transformation, either ingesting data, creating entity-instances or transforming existing data',
    `description` TEXT NOT NULL,
    `repository_url` TEXT NULL COMMENT 'URL to the repository where the transformation script is stored', 
    `commit_hash` CHAR(40) NULL COMMENT 'git commit hash',
    `file_path` TEXT NOT NULL COMMENT 'Path to the transformation script or notebook in the repository',
    `date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `created_by` VARCHAR(255) DEFAULT (CURRENT_USER())
    ) COMMENT = 'Transformations table to record data transformations and ingests';
    """
    DBInterface.execute(conn, sql)
    @info "Created transformations table"
    return nothing
end
"""
    createvariables(conn)

Create tables to record value types, variables and vocabularies
"""
function createvariables(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `value_types` (
        `value_type_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
        `value_type` VARCHAR(80) NOT NULL UNIQUE COMMENT 'As defined in the xsd schema https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes for atomic types and the special value ''enumeration'' for categorical variables',
        `description` TEXT
    ) COMMENT = 'Value types table to record different types of values for variables';
    """
    DBInterface.execute(conn, sql)
    @info "Created value_types table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `vocabularies` (
        `vocabulary_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
        `name` VARCHAR(80) NOT NULL,
        `description` TEXT
    ) COMMENT = 'Vocabularies table to record different vocabularies (integer value and string code) used for categorical variables';
    """
    DBInterface.execute(conn, sql)
    @info "Created vocabularies table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `vocabulary_items` (
        `vocabulary_item_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
        `vocabulary_id` INTEGER NOT NULL,
        `value` INTEGER NOT NULL,
        `code` VARCHAR(80) NOT NULL COMMENT 'String code for the vocabulary item, should comply with xsd:token definition',
        `description` TEXT,
        CONSTRAINT `fk_vocabulary_items` FOREIGN KEY (`vocabulary_id`) REFERENCES `vocabularies`(`vocabulary_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    ) COMMENT = 'Vocabulary items table to record items in vocabularies with integer value and string code';
    """
    DBInterface.execute(conn, sql)
    @info "Created vocabulary_items table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `domains` (
    `domain_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `name` VARCHAR(80) NOT NULL UNIQUE COMMENT 'If it is a public ontology, this is the prefix of the ontology, otherwise it is the name of the namespace',
    `description` TEXT NULL,
    `uri` TEXT NULL COMMENT 'URI to the domain for public ontologies or namespaces'
    ) COMMENT = 'Domains table to record different namespaces for variable, entity and entityrelations identifiers';
    """
    DBInterface.execute(conn, sql)
    @info "Created domains table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `variables` (
        `variable_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
        `domain_id` INTEGER NOT NULL,
        `name` VARCHAR(80) NOT NULL COMMENT 'Name of the variable, should be unique within the domain and comply with xsd:token definition',
        `value_type_id` INTEGER NOT NULL,
        `vocabulary_id` INTEGER COMMENT 'ID of the vocabulary used for categorical variables, NULL for non-categorical variables',
        `description` TEXT,
        `note` TEXT,
        `ontology_namespace` TEXT NULL COMMENT 'Namespace of the ontology for the variable, e.g. http://purl.obolibrary.org/obo/',
        `ontology_class` TEXT NULL COMMENT 'Class identifier of the ontology for the variable, e.g. EFO_0000408',
        CONSTRAINT `fk_variables_domain_id` FOREIGN KEY (`domain_id`) REFERENCES `domains`(`domain_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT `fk_variables_value_type_id` FOREIGN KEY (`value_type_id`) REFERENCES `value_types`(`value_type_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT `fk_variables_vocabulary_id` FOREIGN KEY (`vocabulary_id`) REFERENCES `vocabularies`(`vocabulary_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    ) COMMENT = 'Variables table to record variables with their value types, vocabularies and ontology information';
    """
    DBInterface.execute(conn, sql)
    @info "Created variables table"
    sql = raw"""
    CREATE UNIQUE INDEX IF NOT EXISTS `i_variables_domain_name`
    ON `variables` (`domain_id`,`name`);
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `vocabulary_mapping` (
    `vocabulary_mapping_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `from_vocabulary_item` INTEGER NOT NULL,
    `to_vocabulary_item` INTEGER NOT NULL,
    CONSTRAINT `fk_vocabulary_mapping_from` FOREIGN KEY (`from_vocabulary_item`) REFERENCES `vocabulary_items` (`vocabulary_item_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT `fk_vocabulary_mapping_to` FOREIGN KEY (`to_vocabulary_item`) REFERENCES `vocabulary_items` (`vocabulary_item_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    ) COMMENT = 'Vocabulary mapping table to map equivalent vocabulary items from one vocabulary to another';
    """
    DBInterface.execute(conn, sql)
    @info "Created vocabulary_mapping table"
    types = initvalue_types()
    savedataframe(conn, types, "value_types") # Initialize value types
    @info "Initialized value types"
    return nothing
end
"""
    initvalue_types()

Add default value types
"""
initvalue_types() = DataFrame([(value_type_id=TRE_TYPE_INTEGER, value_type="xsd:integer", description=""),
    (value_type_id=TRE_TYPE_FLOAT, value_type="xsd:float", description=""),
    (value_type_id=TRE_TYPE_STRING, value_type="xsd:string", description=""),
    (value_type_id=TRE_TYPE_DATE, value_type="xsd:date", description="ISO Date yyyy-mm-dd"),
    (value_type_id=TRE_TYPE_DATETIME, value_type="xsd:dateTime", description="ISO Datetime yyyy-mm-ddTHH:mm:ss.sss"),
    (value_type_id=TRE_TYPE_TIME, value_type="xsd:time", description="ISO Time HH:mm:ss.sss"),
    (value_type_id=TRE_TYPE_CATEGORY, value_type="enumeration", description="Category represented by a Vocabulary with integer value and string code, stored as Integer")
])

"""
    updatevariable_vocabulary(conn::DBInterface.Connection, name, domain_id, vocabulary_id)

Update variable vocabulary
"""
function updatevariable_vocabulary(conn::MySQL.Connection, name, domain_id, vocabulary_id)
    sql = """
    UPDATE variables
      SET vocabulary_id = $vocabulary_id
    WHERE name LIKE '%$name%'
      AND domain_id = $domain_id
    """
    DBInterface.execute(conn, sql)
end
"""
    createassets(conn::MySQL.Connection)

Create tables to record data assets, rows, data and links to the transformations that use/created the assets
A digital asset is a dataset or file that is stored in the TRE datalake.
The asset_versions table tracks different versions of the assets, with a version label and note. 
An asset can have multiple versions, and the latest version is flagged by the is_latest flag set as TRUE.
The datasets table is a type of asset that is linked to the asset_versions table and managed through the ducklake extension.
The datafiles table stores references to files in the data lake, with metadata such as compression, encryption, storage URI, format, and digest.
The transformation_inputs and transformation_outputs tables link transformations to the asset versions they use or produce.
The dataset_variables table links datasets to the variables (columns) they contain, representing the schema of the dataset.
The data_asset_entities table links assets to entity instances, allowing for tracking which entities are associated with specific assets.
"""
function createassets(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS assets (
        `asset_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
        `study_id` INTEGER NOT NULL COMMENT 'ID of the study this asset is associated with',
        `name` VARCHAR(255) NOT NULL COMMENT 'Name of the asset, should be unique within the study and comply with xsd:token definition',
        `description` TEXT,
        `asset_type` ENUM('dataset', 'file') NOT NULL COMMENT 'Type of the asset, restricted to dataset or file',
        `date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `created_by` VARCHAR(255) DEFAULT (CURRENT_USER()),
        CONSTRAINT `fk_assets_study_id` FOREIGN KEY (`study_id`) REFERENCES `studies`(`study_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT i_assets_studyname UNIQUE (study_id, name)
    ) COMMENT = 'Assets table to record digital assets such as datasets and files';
    """
    DBInterface.execute(conn, sql)
    @info "Created assets table"
    sql = raw"""
        CREATE TABLE IF NOT EXISTS asset_versions (
            version_id INT AUTO_INCREMENT PRIMARY KEY,
            asset_id INT NOT NULL,
            major INT NOT NULL DEFAULT 1,
            minor INT NOT NULL DEFAULT 0,
            patch INT NOT NULL DEFAULT 0,
            version_label VARCHAR(50) GENERATED ALWAYS AS (
                CONCAT('v',major, '.', minor, '.', patch)
            ) STORED,
            version_note TEXT DEFAULT 'Original version' COMMENT 'Note about the version, e.g. description of changes',
            is_latest BOOLEAN DEFAULT TRUE COMMENT 'Before inserting a new version, set all previous versions to FALSE',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            `created_by` VARCHAR(255) DEFAULT (CURRENT_USER()),
            FOREIGN KEY (asset_id) REFERENCES assets(asset_id),
            UNIQUE(asset_id, major, minor, patch)
        ) COMMENT = 'Used to track different versions of assets';
    """
    DBInterface.execute(conn, sql)
    @info "Created asset_versions table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `datasets` (
        `dataset_id` INTEGER PRIMARY KEY COMMENT 'Always equivalent to version_id of the asset_versions table',
        CONSTRAINT `fk_datasets_version_id` FOREIGN KEY (`dataset_id`) REFERENCES `asset_versions` (`version_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    ) COMMENT = 'Datasets table to record datasets as a type of asset, linked to asset_versions';
    """
    DBInterface.execute(conn, sql)
    @info "Created datasets table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `datafiles` (
        `datafile_id` INTEGER PRIMARY KEY COMMENT 'Always equivalent to version_id of the asset_versions table',
        `compressed` BOOLEAN DEFAULT FALSE COMMENT 'If it is compressed it will use zstd compression', 
        `encrypted` BOOLEAN DEFAULT FALSE COMMENT 'Whether the file is encrypted, default is FALSE',
        `compression_algorithm` VARCHAR(50) DEFAULT 'zstd' COMMENT 'Compression algorithm used, default is zstd',
        `encryption_algorithm` VARCHAR(50) DEFAULT 'AES-256-CBC with PKCS5' COMMENT 'Encryption algorithm used, default is AES-256-CBC with PKCS5',
        `salt` BINARY(8) NULL COMMENT 'Salt used for encryption, if encrypted',
        `storage_uri` TEXT NOT NULL COMMENT 'URI to the file in the data lake, e.g. s3://bucket/path/to/file or file:///path/to/file',
        `edam_format` VARCHAR(255) NOT NULL COMMENT 'EDAM format identifier, e.g. EDAM:format_1234, see: https://edamontology.org/EDAM:format_1234',
        `digest` CHAR(64) NOT NULL COMMENT 'BLAKE3 digest hex string',
        CONSTRAINT `fk_datafiles_version_id` FOREIGN KEY (`datafile_id`) REFERENCES `asset_versions` (`version_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    ) COMMENT = 'Datafiles table to record files in the data lake, linked to asset_versions, files are stored in the data lake by the application';
    """
    DBInterface.execute(conn, sql)
    @info "Created datafiles table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformation_inputs` (
    `transformation_id` INTEGER NOT NULL COMMENT 'ID of the transformation that uses the input',
    `version_id` INTEGER NOT NULL COMMENT 'ID of the digital asset version that is used as input',
    PRIMARY KEY (`transformation_id`, `version_id`),
    CONSTRAINT `fk_transformation_inputs_transformation_id` FOREIGN KEY (`transformation_id`) REFERENCES `transformations` (`transformation_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_transformation_inputs_version_id` FOREIGN KEY (`version_id`) REFERENCES `asset_versions` (`version_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    ) COMMENT = 'Transformation inputs table to link transformations to the digital asset versions they use';
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_inputs table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformation_outputs` (
    `transformation_id` INTEGER NOT NULL COMMENT 'ID of the transformation that produces the output',
    `version_id` INTEGER NOT NULL COMMENT 'ID of the digital asset version that is produced as output',
    PRIMARY KEY (`transformation_id`, `version_id`),
    CONSTRAINT `fk_transformation_outputs_transformation_id` FOREIGN KEY (`transformation_id`) REFERENCES `transformations` (`transformation_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_transformation_outputs_version_id` FOREIGN KEY (`version_id`) REFERENCES `asset_versions` (`version_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    ) COMMENT = 'Transformation outputs table to link transformations to the digital asset versions they produce';
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_outputs table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `dataset_variables` (
    `dataset_id` INTEGER NOT NULL COMMENT 'ID of the dataset, equivalent to version_id of the asset_versions table',
    `variable_id` INTEGER NOT NULL COMMENT 'ID of the variable in the variables table',
    PRIMARY KEY (`dataset_id`, `variable_id`),
    CONSTRAINT `fk_dataset_variables_variable_id` FOREIGN KEY (`variable_id`) REFERENCES `variables` (`variable_id`) ON DELETE NO ACTION ON UPDATE RESTRICT,
    CONSTRAINT `fk_dataset_variables_dataset_id` FOREIGN KEY (`dataset_id`) REFERENCES `datasets` (`dataset_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    ) COMMENT = 'Dataset variables table to link datasets to the variables (columns) they contain, representing the schema of the dataset';
    """
    DBInterface.execute(conn, sql)
    @info "Created dataset_variables table"
    return nothing
end
"""
    createentities(conn)

Create tables to store entities, entity relations, entity instances, and relation instances.
Entities represent individuals, households, or other entities in the TRE.
Entity relations represent relationships between entities, such as family or household relationships.
Entity instances represent specific instances of entities in a study, allowing for tracking of entities across studies.
"""
function createentities(conn::MySQL.Connection)
    #TODO: Add a table to store the entity types, e.g. individual, household, death, birth, etc.
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `entities` (
    `entity_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `domain_id` INTEGER NOT NULL COMMENT 'ID of the domain this entity belongs to',
    `uuid` VARCHAR(36) NOT NULL UNIQUE COMMENT 'UUID of the entity, to ensure global uniqueness across domains',
    `name` VARCHAR(128) NOT NULL COMMENT 'Name of the entity, should be unique within the domain and comply with xsd:token definition',
    `description` TEXT NULL,
    `ontology_namespace` TEXT NULL COMMENT 'Optional namespace of the ontology for the entity, e.g. http://purl.obolibrary.org/obo/',
    `ontology_class` TEXT NULL COMMENT 'Optional class identifier of the ontology for the entity, e.g. EFO_0000408',
    CONSTRAINT i_entities_entityname UNIQUE (domain_id, name),
    CONSTRAINT `fk_entities_domain_id` FOREIGN KEY (`domain_id`) REFERENCES `domains` (`domain_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    ) COMMENT = 'Entities table to record entities such as individuals, households, etc. in the TRE and link them to public ontologies';
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `entityrelations` (
    `entityrelation_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `entity_id_1` INTEGER NOT NULL COMMENT 'ID of the first entity in the relationship',
    `entity_id_2` INTEGER NOT NULL COMMENT 'ID of the second entity in the relationship',
    `domain_id` INTEGER NOT NULL COMMENT 'ID of the domain this entity relation belongs to',
    `uuid` VARCHAR(36) NOT NULL UNIQUE COMMENT 'UUID of the entity relationship, to ensure global uniqueness across domains',
    `name` VARCHAR(128) NOT NULL COMMENT 'Name of the entity relationship, should be unique within the domain and comply with xsd:token definition',
    `description` TEXT NULL,
    `ontology_namespace` TEXT NULL COMMENT 'Optional namespace of the ontology for the entity relationship, e.g. http://purl.obolibrary.org/obo/',
    `ontology_class` TEXT NULL COMMENT 'Optional class identifier of the ontology for the entity relationship, e.g. EFO_0000408',
    CONSTRAINT i_entityrelations_relationname UNIQUE (domain_id, name),
    CONSTRAINT `fk_entityrelationships_entity_id_1` FOREIGN KEY (`entity_id_1`) REFERENCES `entities` (`entity_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_entityrelationships_entity_id_2` FOREIGN KEY (`entity_id_2`) REFERENCES `entities` (`entity_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    ) COMMENT = 'Entity relations table to record relationships between entities, such as family or household relationships, and link them to public ontologies';
    """
    DBInterface.execute(conn, sql)
    @info "Created entityrelations table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `entity_instances` (
    `instance_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `entity_id` INTEGER NOT NULL COMMENT 'ID of the entity this instance belongs to',
    `transformation_id` INTEGER NOT NULL COMMENT 'ID of the transformation that created this entity instance',
    `study_id` INTEGER NOT NULL COMMENT 'ID of the study this entity instance is associated with',
    `external_id` VARCHAR(128) NULL COMMENT 'External identifier for the entity instance, e.g. from a study database, registry or sponsor',
    CONSTRAINT `fk_entity_instances_entity_id` FOREIGN KEY (`entity_id`) REFERENCES `entities` (`entity_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_entity_instances_study_id` FOREIGN KEY (`study_id`) REFERENCES `studies` (`study_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_entity_instances_transformation_id` FOREIGN KEY (`transformation_id`) REFERENCES `transformations` (`transformation_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    ) COMMENT = 'Entity instances table to record specific instances of entities in a study, allowing for tracking of entities across studies';
    """
    DBInterface.execute(conn, sql)
    @info "Created entity_instances table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `relation_instances` (
    `relation_instance_id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `entityrelation_id` INTEGER NOT NULL COMMENT 'ID of the entity relationship this instance belongs to',
    `entity_instance_id_1` BIGINT NOT NULL COMMENT 'ID of the first entity instance in the relationship',
    `entity_instance_id_2` BIGINT NOT NULL COMMENT 'ID of the second entity instance in the relationship',
    `valid_from` DATE NOT NULL COMMENT 'Start date of the relationship instance, e.g. when the relationship episode started',
    `valid_to` DATE NOT NULL COMMENT 'End date of the relationship instance, e.g. when the relationship episode ended',
    `external_id` VARCHAR(128) NULL COMMENT 'External identifier for the relationship instance, e.g. from a study database, registry or sponsor',
    `transformation_id` INTEGER NOT NULL COMMENT 'ID of the transformation that created this entity relation instance',
    CONSTRAINT `fk_relationship_instances_entityrelationship_id` FOREIGN KEY (`entityrelation_id`) REFERENCES `entityrelations` (`entityrelation_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_relationship_instances_entity_instance_id_1` FOREIGN KEY (`entity_instance_id_1`) REFERENCES `entity_instances` (`instance_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_relationship_instances_entity_instance_id_2` FOREIGN KEY (`entity_instance_id_2`) REFERENCES `entity_instances` (`instance_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_relationship_instances_transformation_id` FOREIGN KEY (`transformation_id`) REFERENCES `transformations` (`transformation_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    ) COMMENT = 'Relation instances table to record specific instances of entity relationships, allowing for tracking of relationships between entity instances in a study';
    """
    DBInterface.execute(conn, sql)
    @info "Created relation_instances table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `data_asset_entities` (
     `asset_id` INT NOT NULL COMMENT 'ID of the asset this entity instance is associated with',
     `entity_instance_id` BIGINT NOT NULL COMMENT 'ID of the entity instance this asset is associated with',
    PRIMARY KEY (`asset_id`, `entity_instance_id`),
    CONSTRAINT `fk_data_asset_entities_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`asset_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_data_asset_entities_entity_instance_id` FOREIGN KEY (`entity_instance_id`) REFERENCES `entity_instances` (`instance_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    ) COMMENT = 'Data asset entities table to link assets to entity instances, allowing for tracking which entities are associated with specific assets';
    """
    DBInterface.execute(conn, sql)
    @info "Created data_asset_entities table"
    return nothing
end

"""
    createmapping(conn::MySQL.Connection)

Create the table required for variable mapping. This table is used to map variables from one instrument to another. The table is created in the database provided as an argument.
The variable mapping is based on the PyCrossVA approach.

The relationship to the PyCrossVA configuration file columns:

  * New Column Name = destination_id - the variable_id of the new column
  * New Column Documentation = Stored in the variable table
  * study Column ID = from_id - the variable_id of the study variable
  * study Column Documentation = will be in the variables table
  * Relationship = operator - the operator to be used to create the new variable
  * Condition = operants - the operants to be used with the operator
  * Prerequisite = prerequisite_id - the variable_id of the prerequisite variable

"""
function createmapping(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `variable_mapping` (
    `mapping_id` INTEGER AUTO_INCREMENT PRIMARY KEY,
    `from_variable_id` INTEGER NOT NULL COMMENT 'ID of the variable from the source instrument',
    `to_variable_id` INTEGER NOT NULL COMMENT 'ID of the variable in the destination instrument',
    `operator` ENUM('eq','gt','ge','lt','le','ne','contains','between','map') NOT NULL COMMENT 'Operator to be used to create the variable value',
    `operants` TEXT NOT NULL COMMENT 'Operants to be used with the operator, e.g. the value to compare the variable to, or the mapping to use',
    `prerequisite_id` INTEGER COMMENT 'ID of the prerequisite variable that must be satisfied for the mapping to be applied',
    CONSTRAINT `fk_variable_mapping_from_variable_id` FOREIGN KEY (`from_variable_id`) REFERENCES `variables` (`variable_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_variable_mapping_to_variable_id` FOREIGN KEY (`to_variable_id`) REFERENCES `variables` (`variable_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_variable_mapping_prerequisite_id` FOREIGN KEY (`prerequisite_id`) REFERENCES `variables` (`variable_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    ) COMMENT = 'Variable mapping table to map variables from one instrument to another, based on the PyCrossVA approach';
    """
    DBInterface.execute(conn, sql)
    @info "Created variable_mapping table"
    return nothing
end