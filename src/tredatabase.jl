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
        #createentities(conn)
        #createmapping(conn)
        return nothing
    finally
        DBInterface.close!(conn)
    end
end
"""
    opendatabase(server::AbstractString, user::AbstractString, password::AbstractString, database::AbstractString, lake_data::Union{String,Nothing}=nothing, lake_db::Union{String,Nothing}=nothing)
Open a database connection to a MySQL server with optional DuckDB data lake support.
This function connects to a MySQL server using the provided credentials and database name.
"""
function opendatabase(server::AbstractString, user::AbstractString, password::AbstractString, database::AbstractString,
    lake_data::Union{String,Nothing}=nothing, lake_db::Union{String,Nothing}=nothing)
    conn = DBInterface.connect(MySQL.Connection, server, user, password, database) #, unix_socket="/var/run/mysqld/mysqld.sock"
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
        ddb = DuckDB.conn()
        lake = DBInterface.connect(ddb)
        # Attach the data lake database
        DBInterface.execute(conn, "ATTACH 'ducklake:mysql:host=$(server) port=3306 user=$(user) password=$(password) database=$(lake_db)' AS rda_lake (DATA_PATH '$lake_data', METADATA_SCHEMA 'ducklake_catalog');")
        DBInterface.execute(conn, "USE rda_lake;")
    end
    return conn, lake
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
    paramnames = map(makeparam, columns) # add @ to column name
    sql = "INSERT INTO $table ($(join(columns, ", "))) VALUES ($(join(paramnames, ", ")));"
    return DBInterface.prepare(conn, sql)
end

"""
    updatevalue(conn::MySQL.Connection, table, condition_column, column, condition_value, value)

Update value of column given condition_value in condition_column
"""
function updatevalue(conn::MySQL.Connection, table, condition_column, column, condition_value, value)
    sql = """
        UPDATE $table 
        SET $column = ?
        WHERE $condition_column = ?
        """
    stmt = DBInterface.prepare(conn, sql)
    DBInterface.execute(stmt, (value, condition_value))
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
    `study_type_id` INTEGER NOT NULL PRIMARY KEY,
    `name` VARCHAR(80) NOT NULL,
    `description` TEXT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created study_types table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `studies` (
    `study_id` INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name` varchar(128) NOT NULL,
    `description` TEXT,
    `study_type_id` INTEGER,
    `date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `created_by` VARCHAR(255) DEFAULT (CURRENT_USER()),
    CONSTRAINT `fk_sources_study_type_id` FOREIGN KEY (`study_type_id`) REFERENCES `study_types` (`study_type_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
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
    INSERT INTO study_types (study_type_id, name, description) VALUES
    (1,  'HDSS', 'Health and Demographic Surveillance System'),
    (2,  'COHORT', 'Cohort Study'),
    (3,  'SURVEY', 'Cross-sectional Survey'),
    (4,  'PANEL', 'Longitudinal/Panel Survey'),
    (5,  'CASE_CONTROL', 'Case-Control Study'),
    (6,  'RCT', 'Randomized Controlled Trial'),

    -- Quantitative
    (7,  'QUASI_EXPERIMENTAL', 'Quasi-experimental Study'),
    (8,  'NATURAL_EXPERIMENT', 'Natural Experiment'),
    (9,  'FIELD_EXPERIMENT', 'Field Experiment'),
    (10, 'LAB_EXPERIMENT', 'Laboratory Experiment'),

    -- Qualitative
    (11, 'QUALITATIVE_INTERVIEW', 'In-depth or Key Informant Interviews'),
    (12, 'FOCUS_GROUP', 'Focus Group Discussion'),
    (13, 'ETHNOGRAPHY', 'Ethnographic Study'),
    (14, 'PARTICIPATORY', 'Participatory Action Research'),
    (15, 'CASE_STUDY', 'Case Study'),

    -- Mixed methods
    (16, 'MIXED_METHODS', 'Mixed Methods Study'),

    -- Secondary / Desk Review
    (17, 'SECONDARY_ANALYSIS', 'Secondary Data Analysis'),
    (18, 'DESK_REVIEW', 'Desk or Literature Review'),

    -- Social / Behavioural
    (19, 'TIME_USE', 'Time Use Study'),
    (20, 'DIARY', 'Diary Study'),
    (21, 'LONGITUDINAL_OBSERVATION', 'Longitudinal Observational Study'),

    -- Simulation / Modelling
    (22, 'SIMULATION', 'Simulation Study'),
    (23, 'AGENT_BASED_MODEL', 'Agent-based Modelling'),
    (24, 'STATISTICAL_MODEL', 'Statistical Modelling Study'),
    (25, 'SYSTEM_DYNAMICS', 'System Dynamics Modelling'),

    -- Genomics / Biomedical
    (26, 'GENOMICS', 'Genomics Study'),
    (27, 'MULTIOMICS', 'Multi-omics Study (e.g., proteomics, metabolomics)'),
    (28, 'BIOBANK', 'Biobank-based Study'),
    (29, 'PHARMACOGENOMICS', 'Pharmacogenomics Study');
"""
"""
    createtransformations(conn)

Create tables to record data transformations and data ingests
"""
function createtransformations(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformation_types` (
    `transformation_type_id` INTEGER NOT NULL PRIMARY KEY,
    `name` VARCHAR(80) NOT NULL
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_types table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformation_statuses` (
    `transformation_status_id` INTEGER NOT NULL PRIMARY KEY,
    `name` VARCHAR(80) NOT NULL
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_statuses table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformations` (
    `transformation_id` INTEGER NOT NULL PRIMARY KEY,
    `transformation_type_id` INTEGER NOT NULL,
    `transformation_status_id` INTEGER NOT NULL,
    `description` TEXT NOT NULL,
    `code_reference` BLOB NOT NULL,
    `date_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `created_by` VARCHAR(255) DEFAULT (CURRENT_USER()),
    CONSTRAINT `fk_transformations_transformation_type_id` FOREIGN KEY (`transformation_type_id`) REFERENCES `transformation_types` (`transformation_type_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_transformations_transformation_status_id` FOREIGN KEY (`transformation_status_id`) REFERENCES `transformation_statuses` (`transformation_status_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created transformations table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `data_ingestions` (
    `data_ingestion_id` INTEGER NOT NULL PRIMARY KEY,
    `study_id` INTEGER NOT NULL,
    `date_received` DATE NOT NULL,
    `description` TEXT,
    CONSTRAINT `fk_data_ingestions_source_id` FOREIGN KEY (`study_id`) REFERENCES `studies` (`study_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created data_ingestions table"
    types = inittypes()
    statuses = initstatuses()
    savedataframe(conn, types, "transformation_types")
    savedataframe(conn, statuses, "transformation_statuses")
    @info "Initialized transformation types and statuses"
    return nothing
end
"""
    inittypes()

Default transformation types
"""
inittypes() = DataFrame([(transformation_type_id=TRE_TRANSFORMATION_TYPE_INGEST, name="Raw data ingest"),
    (transformation_type_id=TRE_TRANSFORMATION_TYPE_TRANSFORM, name="Dataset transform")])
"""
    initstatuses()

Default transformation statuses
"""
initstatuses() = DataFrame([(transformation_status_id=TRE_TRANSFORMATION_STATUS_UNVERIFIED, name="Unverified"),
    (transformation_status_id=TRE_TRANSFORMATION_STATUS_VERIFIED, name="Verified")])
"""
    createvariables(conn)

Create tables to record value types, variables and vocabularies
"""
function createvariables(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `value_types` (
        `value_type_id` INTEGER NOT NULL PRIMARY KEY,
        `value_type` VARCHAR(80) NOT NULL UNIQUE,
        `description` TEXT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created value_types table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `vocabularies` (
        `vocabulary_id` INTEGER NOT NULL PRIMARY KEY,
        `name` VARCHAR(80) NOT NULL,
        `description` TEXT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created vocabularies table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `vocabulary_items` (
        `vocabulary_item_id` INTEGER NOT NULL PRIMARY KEY,
        `vocabulary_id` INTEGER NOT NULL,
        `value` INTEGER NOT NULL,
        `code` VARCHAR(80) NOT NULL,
        `description` TEXT,
        CONSTRAINT `fk_vocabulary_items` FOREIGN KEY (`vocabulary_id`) REFERENCES `vocabularies`(`vocabulary_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created vocabulary_items table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `domains` (
    `domain_id` INTEGER NOT NULL PRIMARY KEY,
    `name` VARCHAR(80) NOT NULL UNIQUE,
    `description` TEXT NULL,
    `uri` TEXT NULL
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created domains table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `variables` (
        `variable_id` INTEGER NOT NULL PRIMARY KEY,
        `domain_id` INTEGER NOT NULL,
        `name` VARCHAR(80) NOT NULL,
        `value_type_id` INTEGER NOT NULL,
        `vocabulary_id` INTEGER,
        `description` TEXT,
        `note` TEXT,
        `keyrole` TEXT,
        CONSTRAINT `fk_variables_domain_id` FOREIGN KEY (`domain_id`) REFERENCES `domains`(`domain_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT `fk_variables_value_type_id` FOREIGN KEY (`value_type_id`) REFERENCES `value_types`(`value_type_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT `fk_variables_vocabulary_id` FOREIGN KEY (`vocabulary_id`) REFERENCES `vocabularies`(`vocabulary_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
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
    `vocabulary_mapping_id` INTEGER NOT NULL PRIMARY KEY,
    `from_vocabulary_item` INTEGER NOT NULL,
    `to_vocabulary_item` INTEGER NOT NULL,
    CONSTRAINT `fk_vocabulary_mapping_from` FOREIGN KEY (`from_vocabulary_item`) REFERENCES `vocabulary_items` (`vocabulary_item_id`) ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT `fk_vocabulary_mapping_to` FOREIGN KEY (`to_vocabulary_item`) REFERENCES `vocabulary_items` (`vocabulary_item_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
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
initvalue_types() = DataFrame([(value_type_id=TRE_TYPE_INTEGER, value_type="Integer", description=""),
    (value_type_id=TRE_TYPE_FLOAT, value_type="Float", description=""),
    (value_type_id=TRE_TYPE_STRING, value_type="String", description=""),
    (value_type_id=TRE_TYPE_DATE, value_type="Date", description="ISO Date yyyy-mm-dd"),
    (value_type_id=TRE_TYPE_DATETIME, value_type="Datetime", description="ISO Datetime yyyy-mm-ddTHH:mm:ss.sss"),
    (value_type_id=TRE_TYPE_TIME, value_type="Time", description="ISO Time HH:mm:ss.sss"),
    (value_type_id=TRE_TYPE_CATEGORY, value_type="Categorical", description="Category represented by a Vocabulary with integer value and string code, stored as Integer")
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
"""
function createassets(conn::MySQL.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS asset_types (
        asset_type_id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(128) NOT NULL
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created asset_types table"
    savedataframe(conn, initassettypes(), "asset_types") # Initialize asset types
    sql = raw"""
    CREATE TABLE IF NOT EXISTS assets (
        asset_id INTEGER NOT NULL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        date_created DATE NOT NULL,
        description TEXT,
        asset_type_id INTEGER,
        `doi` TEXT,
        CONSTRAINT fk_assets_asset_type_id FOREIGN KEY (asset_type_id) REFERENCES asset_types (asset_type_id) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created assets table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `datasets` (
        `dataset_id` INTEGER NOT NULL PRIMARY KEY,
        `asset_id` INTEGER UNIQUE NOT NULL,
        CONSTRAINT `fk_datasets_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`asset_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created datasets table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `datafiles` (
        `datafile_id` INTEGER NOT NULL PRIMARY KEY,
        `asset_id` INTEGER UNIQUE NOT NULL,
        `compressed` BOOLEAN DEFAULT FALSE,
        `encrypted` BOOLEAN DEFAULT FALSE,
        `file_name` VARCHAR(255) NOT NULL,
        `file_path` TEXT NOT NULL,
        CONSTRAINT `fk_datafiles_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`asset_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformation_inputs` (
    `transformation_id` INTEGER NOT NULL,
    `asset_id` INTEGER NOT NULL,
    PRIMARY KEY (`transformation_id`, `asset_id`),
    CONSTRAINT `fk_transformation_inputs_transformation_id` FOREIGN KEY (`transformation_id`) REFERENCES `transformations` (`transformation_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_transformation_inputs_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`asset_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_inputs table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `transformation_outputs` (
    `transformation_id` INTEGER NOT NULL,
    `asset_id` INTEGER NOT NULL,
    PRIMARY KEY (`transformation_id`, `asset_id`),
    CONSTRAINT `fk_transformation_outputs_transformation_id` FOREIGN KEY (`transformation_id`) REFERENCES `transformations` (`transformation_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_transformation_outputs_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`asset_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_outputs table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `dataset_variables` (
    `dataset_id` INTEGER NOT NULL,
    `variable_id` INTEGER NOT NULL,
    PRIMARY KEY (`dataset_id`, `variable_id`),
    CONSTRAINT `fk_dataset_variables_variable_id` FOREIGN KEY (`variable_id`) REFERENCES `variables` (`variable_id`) ON DELETE NO ACTION ON UPDATE RESTRICT,
    CONSTRAINT `fk_dataset_variables_dataset_id` FOREIGN KEY (`dataset_id`) REFERENCES `datasets` (`dataset_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created dataset_variables table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `ingest_assets` (
        `ingest_asset_id` INTEGER NOT NULL PRIMARY KEY,
        `data_ingestion_id` INTEGER NOT NULL,
        `transformation_id` INTEGER NOT NULL,
        `asset_id` INTEGER NOT NULL,
        CONSTRAINT `fk_ingest_datasets_data_ingestion_id` FOREIGN KEY (`data_ingestion_id`) REFERENCES `data_ingestions` (`data_ingestion_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
        CONSTRAINT `fk_ingest_datasets_transformation_id` FOREIGN KEY (`transformation_id`) REFERENCES `transformations` (`transformation_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
        CONSTRAINT `fk_ingest_datasets_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`asset_id`) ON DELETE NO ACTION ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created ingest_assets table"
    return nothing
end

"""
    initassettypes()

Create the default asset types for datasets and documents.
"""
initassettypes() = DataFrame([(asset_type_id=TRE_DATASET, name="Dataset"),
    (asset_type_id=TRE_DOCUMENT, name="Document")])
"""
    createentities(conn)

Create tables to store entities
"""
function createentities(conn::MySQL.Connection)
    #TODO: Add a table to store the entity types, e.g. individual, household, death, birth, etc.
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `entities` (
    `entity_id` INTEGER NOT NULL PRIMARY KEY,
    `domain_id` INTEGER NOT NULL,
    `name` VARCHAR(128) NOT NULL,
    `ontology_namespace` TEXT NULL,
    `ontology_class` TEXT NULL,
    CONSTRAINT `fk_entities_domain_id` FOREIGN KEY (`domain_id`) REFERENCES `domains` (`domain_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `entityrelations` (
    `entityrelation_id` INTEGER NOT NULL PRIMARY KEY,
    `entity_id_1` INTEGER NOT NULL,
    `entity_id_2` INTEGER NOT NULL,
    `name` VARCHAR(128) NOT NULL,
    `description` TEXT NULL,
    `ontology_namespace` TEXT NULL,
    `ontology_class` TEXT NULL,
    CONSTRAINT `fk_entityrelationships_entity_id_1` FOREIGN KEY (`entity_id_1`) REFERENCES `entities` (`entity_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_entityrelationships_entity_id_2` FOREIGN KEY (`entity_id_2`) REFERENCES `entities` (`entity_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    );"""
    DBInterface.execute(conn, sql)
    @info "Created entityrelations table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `entity_instances` (
    `instance_id` INTEGER NOT NULL PRIMARY KEY,
    `entity_id` INTEGER NOT NULL,
    `study_id` INTEGER NOT NULL,
    `external_id` VARCHAR(128) NULL,
    CONSTRAINT `fk_entity_instances_entity_id` FOREIGN KEY (`entity_id`) REFERENCES `entities` (`entity_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_entity_instances_study_id` FOREIGN KEY (`study_id`) REFERENCES `studies` (`study_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created entity_instances table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `relation_instances` (
    `relation_instance_id` INTEGER NOT NULL PRIMARY KEY,
    `entityrelation_id` INTEGER NOT NULL,
    `entity_instance_id_1` INTEGER NOT NULL,
    `entity_instance_id_2` INTEGER NOT NULL,
    `valid_from` DATE NOT NULL,
    `valid_to` DATE NOT NULL,
    `external_id` VARCHAR(128) NULL,
    CONSTRAINT `fk_relationship_instances_entityrelationship_id` FOREIGN KEY (`entityrelation_id`) REFERENCES `entityrelationships` (`entityrelation_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_relationship_instances_entity_instance_id_1` FOREIGN KEY (`entity_instance_id_1`) REFERENCES `entity_instances` (`instance_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_relationship_instances_entity_instance_id_2` FOREIGN KEY (`entity_instance_id_2`) REFERENCES `entity_instances` (`instance_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(conn, sql)
    @info "Created relation_instances table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS `data_asset_entities` (
     `asset_id` INTEGER NOT NULL,
    `entity_instance_id` INTEGER NOT NULL,
    PRIMARY KEY (`asset_id`, `entity_instance_id`),
    CONSTRAINT `fk_data_asset_entities_asset_id` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`asset_id`) ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT `fk_data_asset_entities_entity_instance_id` FOREIGN KEY (`entity_instance_id`) REFERENCES `entity_instances` (`instance_id`) ON DELETE CASCADE ON UPDATE RESTRICT
    );
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
    `mapping_id` INTEGER NOT NULL PRIMARY KEY,
    `from_variable_id` INTEGER NOT NULL,
    `to_variable_id` INTEGER NOT NULL,
    `operator` TEXT NOT NULL,
    `operants` TEXT NOT NULL,
    `prerequisite_id` INTEGER,
    CONSTRAINT `fk_variable_mapping_from_variable_id` FOREIGN KEY (`from_variable_id`) REFERENCES `variables` (`variable_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_variable_mapping_to_variable_id` FOREIGN KEY (`to_variable_id`) REFERENCES `variables` (`variable_id`) ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT `fk_variable_mapping_prerequisite_id` FOREIGN KEY (`prerequisite_id`) REFERENCES `variables` (`variable_id`) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(conn, sql)
    return nothing
end