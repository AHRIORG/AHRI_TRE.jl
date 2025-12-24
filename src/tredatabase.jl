"""
    replace_database(conn::DBInterface.Connection, database::String, user::String, password::String)

Replace the existing database with a new one, dropping it first if it exists. 
    This function is used to reset the database, typically for development or testing purposes.
    Creates the user role if it does not exist, and set the rolle as the owner of the new database.
NB: ONLY USE THIS FUNCTION IN DEVELOPMENT OR TESTING ENVIRONMENTS,
    as it will drop the existing database, lake and all its contents.
"""
function replace_database(conn::DBInterface.Connection, database::String, user::String, password::String)
    @info "Recreating database $(database) owned by $(user)"
    try
        DBInterface.execute(conn, "DROP DATABASE IF EXISTS \"$(database)\" WITH (FORCE);")
    catch e
        @warn "Could not drop existing database $(database)" exception = e
    end
    sql_role = """
       DO \$\$
         BEGIN
         EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '$(user)', '$(password)');
         EXCEPTION WHEN duplicate_object THEN
         -- role already exists; no-op
         END
       \$\$;
       """
    DBInterface.execute(conn, sql_role)
    DBInterface.execute(conn, "CREATE DATABASE \"$(database)\" OWNER \"$(user)\";")
end
"""
    createdatabase(conn::DBInterface.Connection; replace=false)

Build the TRE schema objects in an existing PostgreSQL database connection.
This function no longer creates or drops the physical database; that responsibility has been
moved to `createdatastore` (see refactor 2b). The DuckDB/ducklake metadata database creation
code remains untouched in `createdatastore`.
"""
function createdatabase(conn::DBInterface.Connection)
    createstudies(conn)
    createtransformations(conn)
    createvariables(conn)
    createassets(conn)
    createentities(conn)
    # createmapping(conn) should be stored in the DuckDB lake
    return nothing
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
Open a database connection to a PostgreSQL server with optional DuckDB data lake support.
This function connects to a PostgreSQL server using the provided credentials and database name.
"""
function opendatastore(server::AbstractString, user::AbstractString, password::AbstractString, database::AbstractString,
    lake_data::Union{String,Nothing}=nothing, lake_db::Union{String,Nothing}=nothing,
    lake_user::Union{String,Nothing}=nothing, lake_password::Union{String,Nothing}=nothing; port::Integer=5432)
    conn = DBInterface.connect(LibPQ.Connection, "host=$(server) port=$(port) dbname=$(database) user=$(user) password=$(password)")
    @info "Connected to PostgreSQL $(database) on $(server):$(port)"
    lake = nothing
    if !isnothing(lake_data) && !isnothing(lake_db)
        @info "Opening DuckDB data lake at $(lake_data) with metadata database $lake_db"
        if !isdir(lake_data)
            @info "Creating lake data directory $(lake_data)"
            mkpath(lake_data)
        end
        ddb = DuckDB.DB()
        lake = DBInterface.connect(ddb)
        try
            DBInterface.execute(lake, "LOAD ducklake;")
            DBInterface.execute(lake, "LOAD postgres;")
            DBInterface.execute(lake, "UPDATE EXTENSIONS;")
        catch e
            @warn "Failed loading ducklake/postgres extensions" exception = e
        end
        try
            DBInterface.execute(
                lake,
                "ATTACH 'ducklake:postgres:host=$(server) port=$(port) user=$(lake_user) password=$(lake_password) dbname=$(lake_db)' 
                 AS $LAKE_ALIAS (DATA_PATH '$lake_data', METADATA_SCHEMA 'ducklake_catalog');"
            )
            DBInterface.execute(lake, "USE $LAKE_ALIAS;")
        catch e
            @warn "Could not attach DuckDB lake (postgres extension missing?)" exception = e
        end
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
    get_table(conn::DBInterface.Connection, table::String)::AbstractDataFrame

Retrieve table `table` as a DataFrame from `conn`
"""
function get_table(conn::DBInterface.Connection, table::String)::AbstractDataFrame
    sql = "SELECT * FROM $(table)"
    df = DBInterface.execute(conn, sql) |> DataFrame
    return df
end
"""
    makeparam(s)

Return a parameterized string for SQL queries
"""
makeparams(n) = ["\$" * string(i) for i in 1:n]  # e.g. ["$1","$2",...]
"""
    savedataframe(con::DBInterface.Connection, df::AbstractDataFrame, table)

Save a DataFrame to a database table, the names of the dataframe columns should be identical to the table column names in the database
"""
function savedataframe(con::DBInterface.Connection, df::AbstractDataFrame, table)
    colnames = names(df)
    paramnames = makeparams(length(colnames))
    sql = "INSERT INTO $(table) ($(join(colnames, ", "))) VALUES ($(join(paramnames, ", ")));"
    stmt = DBInterface.prepare(con, sql)
    for row in eachrow(df)
        DBInterface.execute(stmt, [row[c] for c in colnames])
    end
end
"""
    prepareinsertstatement(conn::DBInterface.Connection, table, columns)

Prepare an insert statement for PostgreSQL into table for columns
"""
function prepareinsertstatement(conn::DBInterface.Connection, table, columns)
    paramnames = makeparams(length(columns))
    sql = "INSERT INTO $(table) ($(join(columns, ", "))) VALUES ($(join(paramnames, ", ")));"
    return DBInterface.prepare(conn, sql)
end

"""
    updatevalues(conn::DBInterface.Connection, table, condition_column, condition_value, columns, values)

Update value of column given condition_value in condition_column
"""
function updatevalues(conn::DBInterface.Connection, table, condition_column, condition_value, columns, values)
    assigns = [string(col, " = \$", i) for (i, col) in enumerate(columns)]
    sql = string("UPDATE ", table, " SET ", join(assigns, ", "), " WHERE ", condition_column, " = \$", string(length(columns) + 1), ";")
    stmt = DBInterface.prepare(conn, sql)
    DBInterface.execute(stmt, vcat(values, condition_value))
    return nothing
end
"""
    insertwithidentity(conn::DBInterface.Connection, table, columns, values)

Insert a record, returning the identity column value
"""
function insertwithidentity(conn::DBInterface.Connection, table, columns, values)
    params = makeparams(length(columns))
    if endswith(table, "ies")
        base = string(chop(chop(chop(table))), "y")
    elseif endswith(table, "s")
        base = chop(table)
    else
        base = table
    end
    pk = base * "_id"
    sql = string("INSERT INTO ", table, " (", join(columns, ", "), ") VALUES (", join(params, ", "), ") RETURNING \"", pk, "\";")
    stmt = DBInterface.prepare(conn, sql)
    df = DBInterface.execute(stmt, values) |> DataFrame
    return df[1, pk]
end

"""
    insertdata(conn::DBInterface.Connection, table, columns, values)

Insert a set of values into a table, columns list the names of the columns to insert, and values the values to insert
"""
function insertdata(conn::DBInterface.Connection, table, columns, values)
    stmt = prepareinsertstatement(conn, table, columns)
    return DBInterface.execute(stmt, values)
end

"""
    prepareselectstatement(conn::DBInterface.Connection, table, columns::Vector{String}, filter::Vector{String})

Return a statement to select columns from a table, with 0 to n columns to filter on
"""
function prepareselectstatement(conn::DBInterface.Connection, table, columns::Vector{String}, filter::Vector{String})
    # Start with the SELECT clause
    select_clause = string("SELECT ", join(columns, ", "), " FROM ", table)

    # Check if there are any filter conditions and build the WHERE clause
    if isempty(filter)
        return DBInterface.prepare(conn, select_clause)
    else
        conds = [string(col, " = \$", i) for (i, col) in enumerate(filter)]
        return DBInterface.prepare(conn, select_clause * " WHERE " * join(conds, " AND "))
    end
end
"""
    selectdataframe(conn::DBInterface.Connection, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame

Return a dataframe from a table, with 0 to n columns to filter on
"""
function selectdataframe(conn::DBInterface.Connection, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame
    stmt = prepareselectstatement(conn, table, columns, filter)
    return DBInterface.execute(stmt, filtervalues) |> DataFrame
end
"""
    createstudies(conn::DBInterface.Connection)

Creates tables to record a study and associated site/s for deaths contributed to the TRE (PostgreSQL version)
"""
function createstudies(conn::DBInterface.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS study_types (
        study_type_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        name VARCHAR(80) NOT NULL,
        namespace VARCHAR(255) NULL,
        ontology_class VARCHAR(80) NULL,
        description TEXT
    );
    COMMENT ON TABLE study_types IS 'Study types table to record different types of studies contributing data to the TRE';
    COMMENT ON COLUMN study_types.namespace IS 'Namespace of the study type, using http://purl.obolibrary.org/obo/OBI_0500000';
    COMMENT ON COLUMN study_types.ontology_class IS 'Class identifier of the study type, e.g. EFO_0000408';
    """
    DBInterface.execute(conn, sql)
    @info "Created study_types table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS studies (
        study_id UUID PRIMARY KEY DEFAULT uuidv7(),
        name VARCHAR(128) NOT NULL UNIQUE,
        description TEXT,
        external_id VARCHAR(128) NULL,
        study_type_id INTEGER,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT CURRENT_USER,
        CONSTRAINT fk_sources_study_type_id FOREIGN KEY (study_type_id) REFERENCES study_types (study_type_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE studies IS 'Studies table to record information about studies contributing data to the TRE';
    COMMENT ON COLUMN studies.name IS 'Name of the study, must be unique. Keep it short - it will be used as the schema name in the DuckDB lake';
    COMMENT ON COLUMN studies.external_id IS 'External identifier for the study, e.g. from a registry or sponsor';
    COMMENT ON COLUMN studies.study_type_id IS 'Type of study, e.g. HDSS, Cohort, Survey, etc.';
    COMMENT ON COLUMN studies.created_by IS 'User who created the study record';
    """
    DBInterface.execute(conn, sql)
    @info "Created studies table"
    DBInterface.execute(conn, initstudytypes())
    @info "Initialized study types"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS study_access (
        study_id UUID NOT NULL,
        user_id VARCHAR(63) NOT NULL,
        access_level VARCHAR(50) NOT NULL,
        date_granted TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        granted_by VARCHAR(255) DEFAULT CURRENT_USER,
        CONSTRAINT fk_study_access_study_id FOREIGN KEY (study_id) REFERENCES studies (study_id) ON DELETE CASCADE,
        CONSTRAINT pk_study_access PRIMARY KEY (study_id, user_id)
    );
    COMMENT ON TABLE study_access IS 'Access control linking users to studies and used in row level security policies for tables studies and assets';
    COMMENT ON COLUMN study_access.access_level IS 'Access level granted to the user for the study';
    """
    DBInterface.execute(conn, sql)
    @info "Created study_access table"
    sql = raw"""
CREATE TABLE IF NOT EXISTS domains (
    domain_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name VARCHAR(80) NOT NULL,
    uri VARCHAR(255) NULL,
    description TEXT NULL        
);
COMMENT ON TABLE domains IS 'Domains table to record different namespaces for variable, entity and entityrelations identifiers';
COMMENT ON COLUMN domains.name IS 'If it is a public ontology, this is the prefix of the ontology, otherwise it is the name of the namespace';
COMMENT ON COLUMN domains.uri IS 'URI to the domain for public ontologies, can be NULL for private namespaces, in which case name must be unique';
-- Uniqueness for non-NULL uri values
CREATE UNIQUE INDEX ux_domains_nonnull
ON domains (name, uri)
WHERE uri IS NOT NULL;

-- Allow at most one row with uri IS NULL for each name
CREATE UNIQUE INDEX ux_domains_null_once_per_name
ON domains (name)
WHERE uri IS NULL;
"""
    DBInterface.execute(conn, sql)
    @info "Created domains table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS study_domains (
        study_id UUID NOT NULL,
        domain_id INTEGER NOT NULL,
        CONSTRAINT fk_study_domains_study_id FOREIGN KEY (study_id) REFERENCES studies (study_id) ON DELETE CASCADE,
        CONSTRAINT fk_study_domains_domain_id FOREIGN KEY (domain_id) REFERENCES domains (domain_id) ON DELETE RESTRICT, -- Restrict deletion of domains if they are linked to studies
        CONSTRAINT pk_study_domains PRIMARY KEY (study_id, domain_id)
    );
    COMMENT ON TABLE study_domains IS 'Linking table to associate studies with domains';
    COMMENT ON COLUMN study_domains.study_id IS 'ID of the study';
    COMMENT ON COLUMN study_domains.domain_id IS 'ID of the domain';
    """
    DBInterface.execute(conn, sql)
    @info "Created study_domains table"
    return nothing
end

"""
    initstudytypes()

Default study types
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
    createtransformations(conn::DBInterface.Connection)

Create tables to record data transformations and data ingests
"""
function createtransformations(conn::DBInterface.Connection)
    # Create ENUM type if it does not exist
    sql = raw"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transformation_type_enum') THEN
            CREATE TYPE transformation_type_enum AS ENUM ('ingest','transform','entity','export','repository');
        END IF;
    END$$;
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS transformations (
        transformation_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        transformation_type transformation_type_enum NOT NULL,
        description TEXT NOT NULL,
        repository_url TEXT NULL,
        commit_hash VARCHAR(40) NULL,
        file_path TEXT NOT NULL,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT CURRENT_USER
    );
    COMMENT ON TABLE transformations IS 'Transformations table to record data transformations and ingests';
    COMMENT ON COLUMN transformations.transformation_type IS 'Type of transformation, either ingesting data, creating entity-instances, exporting datasets or transforming existing data';
    COMMENT ON COLUMN transformations.repository_url IS 'URL to the repository where the transformation script is stored';
    COMMENT ON COLUMN transformations.commit_hash IS 'git commit hash, usually just the first 7 characters';
    COMMENT ON COLUMN transformations.file_path IS 'Path to the transformation script or notebook in the repository';
    """
    DBInterface.execute(conn, sql)
    @info "Created transformations table"
    return nothing
end
"""
    createvariables(conn)

Create tables to record value types, variables and vocabularies
"""
function createvariables(conn::DBInterface.Connection)
    # Create ENUM type if it does not exist
    sql = raw"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'variable_keyrole_enum') THEN
            CREATE TYPE variable_keyrole_enum AS ENUM ('none','record','external');
        END IF;
    END$$;
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS value_types (
        value_type_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        value_type VARCHAR(80) NOT NULL UNIQUE,
        description TEXT
    );
    COMMENT ON TABLE value_types IS 'Value types table to record different types of values for variables';
    COMMENT ON COLUMN value_types.value_type IS 'As defined in the xsd schema https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes for atomic types and the special value ''enumeration'' for categorical variables';
    """
    DBInterface.execute(conn, sql)
    @info "Created value_types table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS vocabularies (
        vocabulary_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        name VARCHAR(80) NOT NULL,
        description TEXT
    );
    COMMENT ON TABLE vocabularies IS 'Vocabularies table to record different vocabularies (integer value and string code) used for categorical variables';
    """
    DBInterface.execute(conn, sql)
    @info "Created vocabularies table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS vocabulary_items (
        vocabulary_item_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        vocabulary_id INTEGER NOT NULL,
        value INTEGER NOT NULL,
        code VARCHAR(80) NOT NULL,
        description TEXT,
        CONSTRAINT fk_vocabulary_items FOREIGN KEY (vocabulary_id) REFERENCES vocabularies(vocabulary_id)
    );
    COMMENT ON TABLE vocabulary_items IS 'Vocabulary items table to record items in vocabularies with integer value and string code';
    COMMENT ON COLUMN vocabulary_items.code IS 'String code for the vocabulary item, should comply with xsd:token definition';
    """
    DBInterface.execute(conn, sql)
    @info "Created vocabulary_items table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS variables (
        variable_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        domain_id INTEGER NOT NULL,
        name VARCHAR(80) NOT NULL,
        value_type_id INTEGER NOT NULL,
        value_format VARCHAR(80) NULL,
        vocabulary_id INTEGER NULL,
        keyrole variable_keyrole_enum NOT NULL DEFAULT 'none',
        description TEXT,
        note TEXT,
        ontology_namespace VARCHAR(255) NULL,
        ontology_class VARCHAR(255) NULL,
        CONSTRAINT fk_variables_domain_id FOREIGN KEY (domain_id) REFERENCES domains(domain_id),
        CONSTRAINT fk_variables_value_type_id FOREIGN KEY (value_type_id) REFERENCES value_types(value_type_id),
        CONSTRAINT fk_variables_vocabulary_id FOREIGN KEY (vocabulary_id) REFERENCES vocabularies(vocabulary_id)
    );
    COMMENT ON TABLE variables IS 'Variables table to record variables with their value types, vocabularies and ontology information';
    COMMENT ON COLUMN variables.name IS 'Name of the variable, should be unique within the domain and comply with xsd:token definition';
    COMMENT ON COLUMN variables.value_type_id IS 'The type of value for the variable, e.g. integer, float, string, date, datetime, time, enumeration (categorical), multiresponse (multiple categories)';
    COMMENT ON COLUMN variables.value_format IS 'The format for date and time variables, e.g. %Y-%m-%d for date, %H:%M for time, %Y-%m-%d %H:%M:%S for datetime';
    COMMENT ON COLUMN variables.vocabulary_id IS 'ID of the vocabulary used for categorical variables, NULL for non-categorical variables';
    COMMENT ON COLUMN variables.ontology_namespace IS 'Namespace of the ontology for the variable, e.g. http://purl.obolibrary.org/obo/';
    COMMENT ON COLUMN variables.ontology_class IS 'Class identifier of the ontology for the variable, e.g. EFO_0000408';
    COMMENT ON COLUMN variables.keyrole IS 'Key role of the variable, e.g. record key, external key or none';
    """
    DBInterface.execute(conn, sql)
    @info "Created variables table"
    sql = raw"""
    CREATE UNIQUE INDEX IF NOT EXISTS i_variables_domain_name ON variables (domain_id, name);
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS vocabulary_mapping (
        vocabulary_mapping_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        from_vocabulary_item INTEGER NOT NULL,
        to_vocabulary_item INTEGER NOT NULL,
        CONSTRAINT fk_vocabulary_mapping_from FOREIGN KEY (from_vocabulary_item) REFERENCES vocabulary_items (vocabulary_item_id),
        CONSTRAINT fk_vocabulary_mapping_to FOREIGN KEY (to_vocabulary_item) REFERENCES vocabulary_items (vocabulary_item_id)
    );
    COMMENT ON TABLE vocabulary_mapping IS 'Vocabulary mapping table to map equivalent vocabulary items from one vocabulary to another';
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
initvalue_types() = DataFrame([(value_type_id=TRE_TYPE_INTEGER, value_type="xsd:integer", description="integer"),
    (value_type_id=TRE_TYPE_FLOAT, value_type="xsd:float", description="float"),
    (value_type_id=TRE_TYPE_STRING, value_type="xsd:string", description="string"),
    (value_type_id=TRE_TYPE_DATE, value_type="xsd:date", description="ISO Date yyyy-mm-dd"),
    (value_type_id=TRE_TYPE_DATETIME, value_type="xsd:dateTime", description="ISO Datetime yyyy-mm-ddTHH:mm:ss.sss"),
    (value_type_id=TRE_TYPE_TIME, value_type="xsd:time", description="ISO Time HH:mm:ss.sss"),
    (value_type_id=TRE_TYPE_CATEGORY, value_type="enumeration", description="Category represented by a Vocabulary with integer value and string code, stored as Integer"),
    (value_type_id=TRE_TYPE_MULTIRESPONSE, value_type="multiresponse", description="Multi-response enumeration with multiple values, stored as an array of integers")
])

"""
    updatevariable_vocabulary(conn::DBInterface.Connection, name, domain_id, vocabulary_id)

Update variable vocabulary
"""
function updatevariable_vocabulary(conn::DBInterface.Connection, name, domain_id, vocabulary_id)
    sql = raw"UPDATE variables SET vocabulary_id = $1 WHERE name LIKE $2 AND domain_id = $3;"
    stmt = DBInterface.prepare(conn, sql)
    DBInterface.execute(stmt, (vocabulary_id, "%$name%", domain_id))
end
"""
    createassets(conn::DBInterface.Connection)

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
function createassets(conn::DBInterface.Connection)
    # ENUM type for asset_type
    sql = raw"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'asset_type_enum') THEN
            CREATE TYPE asset_type_enum AS ENUM ('dataset','file');
        END IF;
    END$$;
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS assets (
        asset_id UUID PRIMARY KEY DEFAULT uuidv7(),
        study_id UUID NOT NULL,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        asset_type asset_type_enum NOT NULL,
        date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT CURRENT_USER,
        CONSTRAINT fk_assets_study_id FOREIGN KEY (study_id) REFERENCES studies(study_id),
        CONSTRAINT i_assets_studyname UNIQUE (study_id, name)
    );
    COMMENT ON TABLE assets IS 'Assets table to record digital assets such as datasets and files';
    COMMENT ON COLUMN assets.study_id IS 'ID of the study this asset is associated with';
    COMMENT ON COLUMN assets.name IS 'Name of the asset, should be unique within the study and comply with xsd:token definition';
    COMMENT ON COLUMN assets.asset_type IS 'Type of the asset, restricted to dataset or file';
    """
    DBInterface.execute(conn, sql)
    @info "Created assets table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS asset_versions (
        version_id UUID PRIMARY KEY DEFAULT uuidv7(),
        asset_id UUID NOT NULL,
        major INTEGER NOT NULL DEFAULT 1,
        minor INTEGER NOT NULL DEFAULT 0,
        patch INTEGER NOT NULL DEFAULT 0,
        version_label VARCHAR(50) GENERATED ALWAYS AS ('v' || major || '.' || minor || '.' || patch) STORED,
        version_note TEXT DEFAULT 'Original version',
        is_latest BOOLEAN DEFAULT TRUE,
        doi VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255) DEFAULT CURRENT_USER,
        FOREIGN KEY (asset_id) REFERENCES assets(asset_id) ON DELETE CASCADE,
        UNIQUE(asset_id, major, minor, patch)
    );
    COMMENT ON TABLE asset_versions IS 'Used to track different versions of assets';
    COMMENT ON COLUMN asset_versions.version_note IS 'Note about the version, e.g. description of changes';
    COMMENT ON COLUMN asset_versions.doi IS 'Digital Object Identifier for the version, if available';
    COMMENT ON COLUMN asset_versions.is_latest IS 'Is this the latest version?';

    """
    DBInterface.execute(conn, sql)
    @info "Created asset_versions table"
    # Ensure only the inserted version remains marked as latest per asset
    sql = raw"""
    CREATE OR REPLACE FUNCTION set_latest_asset_version() RETURNS trigger AS $$
    BEGIN
        UPDATE asset_versions
           SET is_latest = FALSE
         WHERE asset_id = NEW.asset_id
           AND version_id <> NEW.version_id
           AND is_latest = TRUE;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_asset_versions_set_latest'
        ) THEN
            CREATE TRIGGER trg_asset_versions_set_latest
            AFTER INSERT ON asset_versions
            FOR EACH ROW
            EXECUTE FUNCTION set_latest_asset_version();
        END IF;
    END$$;

    -- Optional: enforce at most one latest per asset_id
    CREATE UNIQUE INDEX IF NOT EXISTS ux_asset_versions_one_latest
        ON asset_versions(asset_id)
        WHERE is_latest;
    """
    DBInterface.execute(conn, sql)
    @info "Created trigger to ensure only one latest version per asset"
    # Prevent changing major/minor/patch after creation
    sql = raw"""
    CREATE OR REPLACE FUNCTION prevent_version_number_update() RETURNS trigger AS $$
    BEGIN
        IF NEW.major IS DISTINCT FROM OLD.major
           OR NEW.minor IS DISTINCT FROM OLD.minor
           OR NEW.patch IS DISTINCT FROM OLD.patch THEN
            RAISE EXCEPTION 'major/minor/patch are immutable once the version is created';
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trg_asset_versions_immutable_numbers'
        ) THEN
            CREATE TRIGGER trg_asset_versions_immutable_numbers
            BEFORE UPDATE ON asset_versions
            FOR EACH ROW
            EXECUTE FUNCTION prevent_version_number_update();
        END IF;
    END$$;
    """
    DBInterface.execute(conn, sql)
    @info "Created trigger to prevent changes to major/minor/patch"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS datasets (
        dataset_id UUID PRIMARY KEY,
        CONSTRAINT fk_datasets_version_id FOREIGN KEY (dataset_id) REFERENCES asset_versions (version_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE datasets IS 'Datasets table to record datasets as a type of asset, linked to asset_versions';
    COMMENT ON COLUMN datasets.dataset_id IS 'Always equivalent to version_id of the asset_versions table';
    """
    DBInterface.execute(conn, sql)
    @info "Created datasets table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS datafiles (
        datafile_id UUID PRIMARY KEY,
        compressed BOOLEAN DEFAULT FALSE,
        encrypted BOOLEAN DEFAULT FALSE,
        compression_algorithm VARCHAR(50) DEFAULT 'zstd',
        encryption_algorithm VARCHAR(50) DEFAULT 'AES-256-CBC with PKCS5',
        salt BYTEA NULL,
        storage_uri TEXT NOT NULL,
        edam_format VARCHAR(255) NOT NULL,
        digest CHAR(64) NOT NULL,
        CONSTRAINT fk_datafiles_version_id FOREIGN KEY (datafile_id) REFERENCES asset_versions (version_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE datafiles IS 'A specific version of a file (binary large object(BLOB)) stored in the data lake';
    COMMENT ON COLUMN datafiles.datafile_id IS 'Always equivalent to version_id of the asset_versions table';
    COMMENT ON COLUMN datafiles.compressed IS 'If it is compressed it will use zstd compression';
    COMMENT ON COLUMN datafiles.encrypted IS 'Whether the file is encrypted, default is FALSE';
    COMMENT ON COLUMN datafiles.compression_algorithm IS 'Compression algorithm used, default is zstd';
    COMMENT ON COLUMN datafiles.encryption_algorithm IS 'Encryption algorithm used, default is AES-256-CBC with PKCS5';
    COMMENT ON COLUMN datafiles.salt IS 'Salt used for encryption, if encrypted';
    COMMENT ON COLUMN datafiles.storage_uri IS 'URI to the file in the data lake, e.g. s3://bucket/path/to/file or file:///path/to/file';
    COMMENT ON COLUMN datafiles.edam_format IS 'EDAM format identifier, e.g. EDAM:format_1234, see: https://edamontology.org/EDAM:format_1234';
    COMMENT ON COLUMN datafiles.digest IS 'SHA-256 digest hex string';
    """
    DBInterface.execute(conn, sql)
    @info "Created datafiles table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS transformation_inputs (
        transformation_id INTEGER NOT NULL,
        version_id UUID NOT NULL,
        PRIMARY KEY (transformation_id, version_id),
        CONSTRAINT fk_transformation_inputs_transformation_id FOREIGN KEY (transformation_id) REFERENCES transformations (transformation_id) ON DELETE CASCADE,
        CONSTRAINT fk_transformation_inputs_version_id FOREIGN KEY (version_id) REFERENCES asset_versions (version_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE transformation_inputs IS 'Transformation inputs table to link transformations to the digital asset versions they use';
    COMMENT ON COLUMN transformation_inputs.transformation_id IS 'ID of the transformation that uses the input';
    COMMENT ON COLUMN transformation_inputs.version_id IS 'ID of the digital asset version that is used as input';
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_inputs table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS transformation_outputs (
        transformation_id INTEGER NOT NULL,
        version_id UUID NOT NULL,
        PRIMARY KEY (transformation_id, version_id),
        CONSTRAINT fk_transformation_outputs_transformation_id FOREIGN KEY (transformation_id) REFERENCES transformations (transformation_id) ON DELETE CASCADE,
        CONSTRAINT fk_transformation_outputs_version_id FOREIGN KEY (version_id) REFERENCES asset_versions (version_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE transformation_outputs IS 'Transformation outputs table to link transformations to the digital asset versions they produce';
    COMMENT ON COLUMN transformation_outputs.transformation_id IS 'ID of the transformation that produces the output';
    COMMENT ON COLUMN transformation_outputs.version_id IS 'ID of the digital asset version that is produced as output';
    """
    DBInterface.execute(conn, sql)
    @info "Created transformation_outputs table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS dataset_variables (
        dataset_id UUID NOT NULL,
        variable_id INTEGER NOT NULL,
        keyrole variable_keyrole_enum NOT NULL DEFAULT 'none',
        PRIMARY KEY (dataset_id, variable_id),
        CONSTRAINT fk_dataset_variables_variable_id FOREIGN KEY (variable_id) REFERENCES variables (variable_id),
        CONSTRAINT fk_dataset_variables_dataset_id FOREIGN KEY (dataset_id) REFERENCES datasets (dataset_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE dataset_variables IS 'Dataset variables table to link datasets to the variables (columns) they contain, representing the schema of the dataset';
    COMMENT ON COLUMN dataset_variables.dataset_id IS 'ID of the dataset, equivalent to version_id of the asset_versions table';
    COMMENT ON COLUMN dataset_variables.variable_id IS 'ID of the variable in the variables table';
    COMMENT ON COLUMN dataset_variables.keyrole IS 'Key role of the variable in the dataset, e.g. record key, external key, or none';
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
function createentities(conn::DBInterface.Connection)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS entities (
        entity_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        domain_id INTEGER NOT NULL,
        uuid UUID DEFAULT uuidv7(),
        name VARCHAR(128) NOT NULL,
        description TEXT NULL,
        ontology_namespace TEXT NULL,
        ontology_class TEXT NULL,
        CONSTRAINT i_entities_entityname UNIQUE (domain_id, name),
        CONSTRAINT fk_entities_domain_id FOREIGN KEY (domain_id) REFERENCES domains (domain_id)
    );
    COMMENT ON TABLE entities IS 'Entities table to record entities such as individuals, households, etc. in the TRE and link them to public ontologies';
    COMMENT ON COLUMN entities.domain_id IS 'ID of the domain this entity belongs to';
    COMMENT ON COLUMN entities.uuid IS 'UUID of the entity, to ensure global uniqueness across domains';
    COMMENT ON COLUMN entities.name IS 'Name of the entity, should be unique within the domain and comply with xsd:NCName definition';
    COMMENT ON COLUMN entities.ontology_namespace IS 'Optional namespace of the ontology for the entity, e.g. http://purl.obolibrary.org/obo/';
    COMMENT ON COLUMN entities.ontology_class IS 'Optional class identifier of the ontology for the entity, e.g. EFO_0000408';
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS entityrelations (
        entityrelation_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        subject_entity_id INTEGER NOT NULL,
        object_entity_id INTEGER NOT NULL,
        domain_id INTEGER NOT NULL,
        uuid UUID DEFAULT uuidv7(),
        name VARCHAR(128) NOT NULL,
        description TEXT NULL,
        ontology_namespace TEXT NULL,
        ontology_class TEXT NULL,
        CONSTRAINT i_entityrelations_relationname UNIQUE (domain_id, name),
        CONSTRAINT fk_entityrelationships_entity_id_1 FOREIGN KEY (subject_entity_id) REFERENCES entities (entity_id) ON DELETE CASCADE,
        CONSTRAINT fk_entityrelationships_entity_id_2 FOREIGN KEY (object_entity_id) REFERENCES entities (entity_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE entityrelations IS 'Entity relations table to record relationships between entities, such as family or household relationships, and link them to public ontologies';
    COMMENT ON COLUMN entityrelations.subject_entity_id IS 'The entity being described';
    COMMENT ON COLUMN entityrelations.object_entity_id IS 'The entity that is related to the subject entity';
    COMMENT ON COLUMN entityrelations.domain_id IS 'ID of the domain this entity relation belongs to';
    COMMENT ON COLUMN entityrelations.uuid IS 'UUID of the entity relationship, to ensure global uniqueness across domains';
    COMMENT ON COLUMN entityrelations.name IS 'Name of the entity relationship, should be unique within the domain and comply with xsd:token definition';
    COMMENT ON COLUMN entityrelations.ontology_namespace IS 'Optional namespace of the ontology for the entity relationship, e.g. http://purl.obolibrary.org/obo/';
    COMMENT ON COLUMN entityrelations.ontology_class IS 'Optional class identifier of the ontology for the entity relationship, e.g. EFO_0000408';
    """
    DBInterface.execute(conn, sql)
    @info "Created entityrelations table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS entity_instances (
        instance_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        entity_id INTEGER NOT NULL,
        study_id UUID NOT NULL,
        external_id VARCHAR(128) NULL,
        transformation_id INTEGER NOT NULL,
        CONSTRAINT fk_entity_instances_entity_id FOREIGN KEY (entity_id) REFERENCES entities (entity_id) ON DELETE CASCADE,
        CONSTRAINT fk_entity_instances_study_id FOREIGN KEY (study_id) REFERENCES studies (study_id) ON DELETE CASCADE,
        CONSTRAINT fk_entity_instances_transformation_id FOREIGN KEY (transformation_id) REFERENCES transformations (transformation_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE entity_instances IS 'Entity instances table to record specific instances of entities in a study, allowing for tracking of entities across studies';
    COMMENT ON COLUMN entity_instances.entity_id IS 'ID of the entity this instance belongs to';
    COMMENT ON COLUMN entity_instances.transformation_id IS 'ID of the transformation that created this entity instance';
    COMMENT ON COLUMN entity_instances.study_id IS 'ID of the study this entity instance is associated with';
    COMMENT ON COLUMN entity_instances.external_id IS 'External identifier for the entity instance, e.g. from a study database, registry or sponsor';
    """
    DBInterface.execute(conn, sql)
    @info "Created entity_instances table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS relation_instances (
        relation_instance_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        entityrelation_id INTEGER NOT NULL,
        entity_instance_id_1 BIGINT NOT NULL,
        entity_instance_id_2 BIGINT NOT NULL,
        valid_from DATE NOT NULL,
        valid_to DATE NOT NULL,
        external_id VARCHAR(128) NULL,
        transformation_id INTEGER NOT NULL,
        CONSTRAINT fk_relationship_instances_entityrelationship_id FOREIGN KEY (entityrelation_id) REFERENCES entityrelations (entityrelation_id) ON DELETE CASCADE,
        CONSTRAINT fk_relationship_instances_entity_instance_id_1 FOREIGN KEY (entity_instance_id_1) REFERENCES entity_instances (instance_id) ON DELETE CASCADE,
        CONSTRAINT fk_relationship_instances_entity_instance_id_2 FOREIGN KEY (entity_instance_id_2) REFERENCES entity_instances (instance_id) ON DELETE CASCADE,
        CONSTRAINT fk_relationship_instances_transformation_id FOREIGN KEY (transformation_id) REFERENCES transformations (transformation_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE relation_instances IS 'Relation instances table to record specific instances of entity relationships, allowing for tracking of relationships between entity instances in a study';
    COMMENT ON COLUMN relation_instances.entityrelation_id IS 'ID of the entity relationship this instance belongs to';
    COMMENT ON COLUMN relation_instances.entity_instance_id_1 IS 'ID of the first entity instance in the relationship';
    COMMENT ON COLUMN relation_instances.entity_instance_id_2 IS 'ID of the second entity instance in the relationship';
    COMMENT ON COLUMN relation_instances.valid_from IS 'Start date of the relationship instance, e.g. when the relationship episode started';
    COMMENT ON COLUMN relation_instances.valid_to IS 'End date of the relationship instance, e.g. when the relationship episode ended';
    COMMENT ON COLUMN relation_instances.external_id IS 'External identifier for the relationship instance, e.g. from a study database, registry or sponsor';
    COMMENT ON COLUMN relation_instances.transformation_id IS 'ID of the transformation that created this entity relation instance';
    """
    DBInterface.execute(conn, sql)
    @info "Created relation_instances table"
    sql = raw"""
    CREATE TABLE IF NOT EXISTS data_asset_entities (
        asset_id UUID NOT NULL,
        entity_instance_id BIGINT NOT NULL,
        PRIMARY KEY (asset_id, entity_instance_id),
        CONSTRAINT fk_data_asset_entities_asset_id FOREIGN KEY (asset_id) REFERENCES assets (asset_id) ON DELETE CASCADE,
        CONSTRAINT fk_data_asset_entities_entity_instance_id FOREIGN KEY (entity_instance_id) REFERENCES entity_instances (instance_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE data_asset_entities IS 'Data asset entities table to link assets to entity instances, to track instances associated with an asset';
    COMMENT ON COLUMN data_asset_entities.asset_id IS 'ID of the asset this entity instance is associated with';
    COMMENT ON COLUMN data_asset_entities.entity_instance_id IS 'ID of the entity instance this asset is associated with';
    """
    DBInterface.execute(conn, sql)
    @info "Created data_asset_entities table"
    return nothing
end

"""
    createmapping(conn::DBInterface.Connection)

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
function createmapping(conn::DBInterface.Connection)
    # Create ENUM for operator
    sql = raw"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'operator_enum') THEN
            CREATE TYPE operator_enum AS ENUM ('eq','gt','ge','lt','le','ne','contains','between','map');
        END IF;
    END$$;
    """
    DBInterface.execute(conn, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS variable_mapping (
        mapping_id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        from_variable_id INTEGER NOT NULL,
        to_variable_id INTEGER NOT NULL,
        operator operator_enum NOT NULL,
        operants TEXT NOT NULL,
        prerequisite_id INTEGER NULL,
        CONSTRAINT fk_variable_mapping_from_variable_id FOREIGN KEY (from_variable_id) REFERENCES variables (variable_id) ON DELETE CASCADE,
        CONSTRAINT fk_variable_mapping_to_variable_id FOREIGN KEY (to_variable_id) REFERENCES variables (variable_id) ON DELETE CASCADE,
        CONSTRAINT fk_variable_mapping_prerequisite_id FOREIGN KEY (prerequisite_id) REFERENCES variables (variable_id) ON DELETE CASCADE
    );
    COMMENT ON TABLE variable_mapping IS 'Variable mapping table to map variables from one instrument to another, based on the PyCrossVA approach';
    COMMENT ON COLUMN variable_mapping.from_variable_id IS 'ID of the variable from the source instrument';
    COMMENT ON COLUMN variable_mapping.to_variable_id IS 'ID of the variable in the destination instrument';
    COMMENT ON COLUMN variable_mapping.operator IS 'Operator to be used to create the variable value';
    COMMENT ON COLUMN variable_mapping.operants IS 'Operants to be used with the operator, e.g. the value to compare the variable to, or the mapping to use';
    COMMENT ON COLUMN variable_mapping.prerequisite_id IS 'ID of the prerequisite variable that must be satisfied for the mapping to be applied';
    """
    DBInterface.execute(conn, sql)
    @info "Created variable_mapping table"
    return nothing
end
"""
    transaction_begin(store::DataStore; on_lake::Bool=false)

Begin a transaction on the specified DataStore or its lake connection.
- 'store' is the DataStore object containing the database connection.
- 'on_lake' indicates whether to use the lake connection (true) or the main store connection (false).
If 'on_lake' is true, it begins a transaction on the lake connection.
If 'on_lake' is false, it begins a transaction on the main data store connection.
Returns nothing.
"""
function transaction_begin(store::DataStore; on_lake::Bool=false)
    on_lake ? transaction_begin(store.lake) : transaction_begin(store.store)
end
"""
    transaction_begin(conn::DBInterface.Connection)

Begin a transaction on the specified DBInterface.Connection.
- 'conn' is the DBInterface.Connection object representing the database connection.
Returns nothing.
"""
function transaction_begin(conn::DBInterface.Connection)
    DBInterface.execute(conn, "BEGIN")
    @info "Transaction started"
    return nothing
end
"""
    transaction_commit(store::DataStore; on_lake::Bool=false)

Commit a transaction on the specified DataStore or its lake connection.
- 'store' is the DataStore object containing the database connection.
- 'on_lake' indicates whether to use the lake connection (true) or the main store connection (false).
If 'on_lake' is true, it commits the transaction on the lake connection.
If 'on_lake' is false, it commits the transaction on the main data store connection.
Returns nothing.
"""
function transaction_commit(store::DataStore; on_lake::Bool=false)
    on_lake ? transaction_commit(store.lake) : transaction_commit(store.store)
end
"""
    transaction_commit(conn::DBInterface.Connection; on_lake::Bool=false)

Commit a transaction on the specified DBInterface.Connection.
- 'conn' is the DBInterface.Connection object representing the database connection.
Returns nothing.
"""
function transaction_commit(conn::DBInterface.Connection; on_lake::Bool=false)
    DBInterface.execute(conn, "COMMIT")
    @info "Transaction committed"
    return nothing
end
"""
    transaction_rollback(store::DataStore; on_lake::Bool=false)

Rollback a transaction on the specified DataStore or its lake connection.
- 'store' is the DataStore object containing the database connection.
- 'on_lake' indicates whether to use the lake connection (true) or the main store connection (false).
If 'on_lake' is true, it rolls back the transaction on the lake connection.
If 'on_lake' is false, it rolls back the transaction on the main data store connection.
Returns nothing.
"""
function transaction_rollback(store::DataStore; on_lake::Bool=false)
    on_lake ? transaction_rollback(store.lake) : transaction_rollback(store.store)
end
"""
    transaction_rollback(conn::DBInterface.Connection; on_lake::Bool=false)

Rollback a transaction on the specified DBInterface.Connection.
- 'conn' is the DBInterface.Connection object representing the database connection.
Returns nothing.
"""
function transaction_rollback(conn::DBInterface.Connection; on_lake::Bool=false)
    try
        DBInterface.execute(conn, "ROLLBACK")
        @info "Transaction rolled back"
    catch rollback_err
        @warn "Failed to rollback transaction: $rollback_err"
    end
    return nothing
end
"""
    upsert_study!(store::DataStore, study::Study)::Study

Create or update a study record. If a study with the same name already exists, it updates and returns the study.
Otherwise, it inserts a new row and returns the new study.
If `study.study_id` is `nothing`, it inserts a new study and lets PostgreSQL assign
- 'study' is a Study object containing the name, description, external_id, and study_type_id.
- 'store' is the DataStore object containing the database connection.
If the study name is required and it must be unique.
"""
function upsert_study!(store::DataStore, study::Study)::Study
    if isnothing(store)
        throw(ArgumentError("DataStore cannot be nothing"))
    end
    db = store.store
    if study.study_id === nothing
        # Insert letting PostgreSQL assign uuidv7() default
        @info "Inserting new study: $(study.name)"
        sql = raw"""
        INSERT INTO studies (name, description, external_id, study_type_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (name) DO UPDATE
        SET description   = EXCLUDED.description,
            external_id   = EXCLUDED.external_id,
            study_type_id = EXCLUDED.study_type_id
        RETURNING study_id;
        """
        stmt = DBInterface.prepare(db, sql)
        df = DBInterface.execute(stmt, (study.name, study.description, study.external_id, study.study_type_id)) |> DataFrame
        study.study_id = UUID(df[1, :study_id])
    else
        @info "Updating existing study: $(study.name) with ID $(study.study_id)"
        updatevalues(db, "studies", "study_id", study.study_id, ["name", "description", "external_id", "study_type_id"],
            [study.name, study.description, study.external_id, study.study_type_id])
    end
    return study
end
"""
    get_study(store::DataStore, id::UUID)::Union{Study,Nothing}

Return a Study object by its UUID in the specified DataStore.
- `store` is the DataStore object containing the database connection.
- `id` is the UUID of the study to search for.
If no study is found, it returns `nothing`.
"""
function get_study(store::DataStore, id::UUID)::Union{Study,Nothing}
    db = store.store
    sql = raw"""
        SELECT study_id, name, description, external_id, study_type_id
          FROM studies
         WHERE study_id = $1
         LIMIT 1;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (string(id),)) |> DataFrame
    if nrow(df) == 0
        @info "No study found with ID: $(id)"
        return nothing
    end
    row = df[1, :]
    study = Study(
        study_id=UUID(row.study_id),
        name=row.name,
        description=coalesce(row.description, missing),
        external_id=row.external_id,
        study_type_id=row.study_type_id
    )
    study.domains = get_study_domains(store, study)
    return study
end
"""
    get_study(store::DataStore, name::AbstractString)::Union{Study,Nothing}

Return a Study object by its name in the specified DataStore.
- 'store' is the DataStore object containing the database connection.
- 'name' is the name of the study to search for.
If no study is found, it returns `nothing`.
If a study is found, it returns a Study object
"""
function get_study(store::DataStore, name::AbstractString)::Union{Study,Nothing}
    db = store.store
    sql = raw"""
        SELECT study_id, name, description, external_id, study_type_id
          FROM studies
         WHERE name = $1
         LIMIT 1;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (name,)) |> DataFrame
    if nrow(df) == 0
        @info "No study found with name: $(name)"
        return nothing
    end
    row = df[1, :]
    study = Study(
        study_id=UUID(row.study_id),
        name=row.name,
        description=coalesce(row.description, missing),
        external_id=row.external_id,
        study_type_id=row.study_type_id
    )
    study.domains = get_study_domains(store, study)
    return study
end
"""
    list_studies(store::DataStore)::Vector{Study}

Return a vector of all Study objects in the specified DataStore.
- 'store' is the DataStore object containing the database connection.
This function retrieves all studies from the database, ordered by name.
"""
function list_studies(store::DataStore)::Vector{Study}
    db = store.store
    sql = raw"""
        SELECT study_id, name, description, external_id, study_type_id
          FROM studies
         ORDER BY name;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt) |> DataFrame
    return [Study(
        study_id=UUID(row.study_id),
        name=row.name,
        description=coalesce(row.description, missing),
        external_id=row.external_id,
        study_type_id=row.study_type_id
    ) for row in eachrow(df)]
end
"""
    upsert_domain!(store::DataStore, domain::Domain)::Domain

Create or update a domain record. If a domain with the same (name, uri) already
exists (treating NULL uri correctly), it updates and returns its domain_id.
Otherwise, it inserts a new row and returns the new domain_id.
- 'domain' is a Domain object containing the name, uri, and description.
- 'store' is the DataStore object containing the database connection.
If the domain has a non-NULL URI, it must be unique with respect to the name and URI combination.
If the domain has a NULL URI, it must be unique with respect to the name only, allowing at most one row with a NULL URI for each name.
This function returns the updated or newly created Domain object with the domain_id set.
"""
function upsert_domain!(store::DataStore, domain::Domain)::Domain
    if isnothing(store)
        throw(ArgumentError("DataStore cannot be nothing"))
    end
    db = store.store

    # 1) Does a matching domain already exist?
    sql_get = raw"""
        SELECT domain_id
        FROM domains
        WHERE name = $1
          AND ( ($2::text IS NULL AND uri IS NULL) OR uri = $2 )
        LIMIT 1;
    """
    stmt_get = DBInterface.prepare(db, sql_get)
    df = DBInterface.execute(stmt_get, (domain.name, domain.uri)) |> DataFrame

    if nrow(df) == 0
        @info "Insert new domain: $(domain.name) with URI '$(domain.uri)'"
        sql_ins = raw"""
            INSERT INTO domains (name, uri, description)
            VALUES ($1, $2, $3)
            RETURNING domain_id;
        """
        stmt_ins = DBInterface.prepare(db, sql_ins)
        ins = DBInterface.execute(stmt_ins, (domain.name, domain.uri, domain.description)) |> DataFrame
        domain.domain_id = ins[1, :domain_id]
    else
        @info "Update existing domain: $(domain.name) with URI '$(domain.uri)'"
        domain.domain_id = df[1, :domain_id]
        sql_upd = raw"""
            UPDATE domains
               SET description = $1
             WHERE domain_id   = $2
             RETURNING domain_id;
        """
        stmt_upd = DBInterface.prepare(db, sql_upd)
        DBInterface.execute(stmt_upd, (domain.description, domain.domain_id))
    end

    return domain
end

"""
    get_domain(store::DataStore, name::AbstractString; uri::Union{Nothing,String}=nothing)::Union{Domain,Nothing}

Return a Domain object by its name (and optional URI) in the specified DataStore.
If `uri` is `nothing`, it searches for the domain by name only.
If `uri` is provided, it searches for the domain by both name and URI.
If no domain is found, it returns `nothing`.
If a domain is found, it returns a Domain object with the domain_id, name, uri, and description.
- 'store' is the DataStore object containing the database connection.
- 'name' is the name of the domain to search for.
- 'uri' is an optional URI to further filter the domain search.
"""
function get_domain(store::DataStore, name::AbstractString; uri::Union{Nothing,String}=nothing)::Union{Domain,Nothing}
    db = store.store
    if isnothing(uri)
        sql = raw"""
            SELECT domain_id, name, uri, description
              FROM domains
             WHERE name = $1
             LIMIT 1;
            """
    else
        sql = raw"""
            SELECT domain_id, name, uri, description
              FROM domains
             WHERE name = $1
               AND uri = $2 
             LIMIT 1;
            """
    end
    stmt = DBInterface.prepare(db, sql)
    if isnothing(uri)
        df = DBInterface.execute(stmt, (name,)) |> DataFrame
    else
        df = DBInterface.execute(stmt, (name, uri)) |> DataFrame
    end
    if nrow(df) == 0
        @info "No domain found with name: $(name) and URI: $(uri)"
        return nothing
    end
    row = df[1, :]
    return Domain(
        domain_id=row.domain_id,
        name=row.name,
        uri=coalesce(row.uri, missing),
        description=coalesce(row.description, missing)
    )
end
"""
    add_study_domain!(store::DataStore, study::Study, domain::Domain)

Add a domain to a study by inserting a record into the study_domains table.
- 'store' is the DataStore object containing the database connection.
- 'study' is the Study object containing the study_id.
- 'domain' is the Domain object containing the domain_id.
If the combination of (study_id, domain_id) already exists, it does nothing.
If the domain is not already in the study.domains list, it adds it to the study.domains vector.
"""
function add_study_domain!(store::DataStore, study::Study, domain::Domain)
    db = store.store
    sql = raw"""
        INSERT INTO study_domains (study_id, domain_id)
        VALUES ($1, $2)
        ON CONFLICT (study_id, domain_id) DO NOTHING;
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (study.study_id, domain.domain_id))
    @info "Added domain $(domain.name) to study $(study.name)"
    # Add domain to study.domains, if it doesn't already exist
    if !(domain in study.domains)
        push!(study.domains, domain)
    end
    return nothing
end
function get_study_domains(store::DataStore, study::Study)::Vector{Domain}
    db = store.store
    sql = raw"""
        SELECT d.domain_id, d.name, d.uri, d.description
          FROM domains d
          JOIN study_domains sd ON d.domain_id = sd.domain_id
         WHERE sd.study_id = $1
         ORDER BY d.name;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (study.study_id,)) |> DataFrame
    domains = Domain[]
    for row in eachrow(df)
        push!(domains, Domain(
            domain_id=row.domain_id,
            name=row.name,
            uri=coalesce(row.uri, missing),
            description=coalesce(row.description, missing)
        ))
    end
    return domains
end
"""
    upsert_entity!(store::DataStore, entity::Entity)::Entity

Create or update an entity record. If an entity with the same (domain_id, name) already exists, it updates and returns its entity_id.
- 'entity' is an Entity object containing the domain_id, name, description, ontology_namespace, and ontology_class.
- 'store' is the DataStore object containing the database connection.
If the entity has a non-NULL ontology_namespace and ontology_class, it must be unique with respect to the (domain_id, name) combination.
If the entity has a NULL ontology_namespace or ontology_class, it must be unique with respect to the (domain_id, name) combination, 
allowing at most one row with a NULL ontology_namespace or ontology_class for each (domain_id, name).  
"""
function upsert_entity!(store::DataStore, entity::Entity)::Entity
    conn = store.store
    if entity.entity_id === nothing
        @info "Inserting new entity: $(entity.name) in domain $(entity.domain.name)"
        sql = raw"""
            INSERT INTO entities (domain_id, name, description, ontology_namespace, ontology_class)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (domain_id, name) DO UPDATE
              SET description = EXCLUDED.description,
                  ontology_namespace = EXCLUDED.ontology_namespace,
                  ontology_class = EXCLUDED.ontology_class
            RETURNING entity_id, uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (entity.domain.domain_id, entity.name, entity.description, entity.ontology_namespace, entity.ontology_class)) |> DataFrame
        @info "Rows affected: $(nrow(df))"
        entity.entity_id = df[1, :entity_id]
        entity.uuid = UUID(df[1, :uuid])
    else
        sql = raw"""
            UPDATE entities
               SET domain_id = $2,
                   name = $3,
                   description = $4,
                   ontology_namespace = $5,
                   ontology_class = $6
             WHERE entity_id = $1
             RETURNING uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (entity.entity_id, entity.domain_id, entity.name, entity.description, entity.ontology_namespace, entity.ontology_class)) |> DataFrame
        entity.uuid = UUID(df[1, :uuid])
    end
    return entity
end

"""
    upsert_entityrelation!(store::DataStore, r::EntityRelation)::EntityRelation

Create or update an entity relation record. If a relation with the same (domain_id, name) already exists, it updates and returns its entityrelation_id.
- 'r' is an EntityRelation object containing the entity_id_1, entity_id_2, domain_id, name, description, ontology_namespace, and ontology_class.
- 'store' is the DataStore object containing the database connection.
If the relation has a non-NULL ontology_namespace and ontology_class, it must be unique with respect to the (domain_id, name) combination.
If the relation has a NULL ontology_namespace or ontology_class, it must be unique with respect to the (domain_id, name) combination,
allowing at most one row with a NULL ontology_namespace or ontology_class for each (domain_id, name).
This function returns the updated or newly created EntityRelation object with the entityrelation_id and uuid set.
"""
function upsert_entityrelation!(store::DataStore, r::EntityRelation)::EntityRelation
    conn = store.store
    if r.entityrelation_id === nothing
        sql = raw"""
            INSERT INTO entityrelations
              (subject_entity_id, object_entity_id, domain_id, name, description, ontology_namespace, ontology_class)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (domain_id, name) DO UPDATE
              SET description = EXCLUDED.description,
                  ontology_namespace = EXCLUDED.ontology_namespace,
                  ontology_class = EXCLUDED.ontology_class
            RETURNING entityrelation_id, uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (r.subject_entity.entity_id, r.object_entity.entity_id, r.domain.domain_id, r.name, r.description, r.ontology_namespace, r.ontology_class)) |> DataFrame
        r.entityrelation_id = df[1, :entityrelation_id]
        r.uuid = UUID(df[1, :uuid])
    else
        sql = raw"""
            UPDATE entityrelations
               SET entity_id_1 = $2,
                   entity_id_2 = $3,
                   domain_id = $4,
                   name = $5,
                   description = $6,
                   ontology_namespace = $7,
                   ontology_class = $8
             WHERE entityrelation_id = $1
             RETURNING uuid;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql),
            (r.entityrelation_id, r.entity_id_1, r.entity_id_2, r.domain_id, r.name, r.description, r.ontology_namespace, r.ontology_class)) |> DataFrame
        r.uuid = UUID(df[1, :uuid])
    end
    return r
end
"""
    get_entity(store::DataStore, domain_id::Int, name::String)::Union{Entity,Nothing}

Return an Entity object by its name in the specified domain.
- 'store' is the DataStore object containing the database connection.
- 'domain_id' is the ID of the domain to search in.
- 'name' is the name of the entity to search for.
If no entity is found, it returns `nothing`.
"""
function get_entity(store::DataStore, domain_id::Int, name::String)::Union{Entity,Nothing}
    conn = store.store
    sql = raw"""
        SELECT entity_id, domain_id, uuid, name, description, ontology_namespace, ontology_class
          FROM entities
         WHERE domain_id = $1 AND name = $2
         LIMIT 1;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id, name)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    return Entity(entity_id=row.entity_id,
        domain_id=row.domain_id,
        uuid=UUID(row.uuid),
        name=row.name,
        description=coalesce(row.description, missing),
        ontology_namespace=coalesce(row.ontology_namespace, missing),
        ontology_class=coalesce(row.ontology_class, missing))
end

"""
    get_entityrelation(store::DataStore, domain_id::Int, name::String)::Union{EntityRelation,Nothing}

Return an EntityRelation object by its name in the specified domain.
- 'store' is the DataStore object containing the database connection.
- 'domain_id' is the ID of the domain to search in.
- 'name' is the name of the entity relation to search for.
If no entity relation is found, it returns `nothing`.
If an entity relation is found, it returns an EntityRelation object
"""
function get_entityrelation(store::DataStore, domain_id::Int, name::String)::Union{EntityRelation,Nothing}
    conn = store.store
    sql = raw"""
        SELECT entityrelation_id, entity_id_1, entity_id_2, domain_id, uuid, name,
               description, ontology_namespace, ontology_class
          FROM entityrelations
         WHERE domain_id = $1 AND name = $2
         LIMIT 1;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id, name)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    return EntityRelation(
        entityrelation_id=row.entityrelation_id,
        entity_id_1=row.entity_id_1,
        entity_id_2=row.entity_id_2,
        domain_id=row.domain_id,
        uuid=UUID(row.uuid),
        name=row.name,
        description=coalesce(row.description, missing),
        ontology_namespace=coalesce(row.ontology_namespace, missing),
        ontology_class=coalesce(row.ontology_class, missing)
    )
end
"""
    list_domainentities(store::DataStore, domain_id::Int)::DataFrame

Return a DataFrame containing all entities in the specified domain.
- 'store' is the DataStore object containing the database connection.
- 'domain_id' is the ID of the domain to list entities from.
The DataFrame will contain all columns from the entities table, ordered by name.
This function is useful for retrieving all entities in a domain for further processing or display.
"""
function list_domainentities(store::DataStore, domain_id::Int)::DataFrame
    conn = store.store
    sql = raw"""
        SELECT * FROM entities
        WHERE domain_id = $1
        ORDER BY name;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id,)) |> DataFrame
    return df
end
"""
    list_domainrelations(store::DataStore, domain_id::Int)::DataFrame

Return a DataFrame containing all entity relations in the specified domain.
- 'store' is the DataStore object containing the database connection.
- 'domain_id' is the ID of the domain to list entity relations from.
The DataFrame will contain all columns from the entityrelations table, ordered by name.
"""
function list_domainrelations(store::DataStore, domain_id::Int)::DataFrame
    conn = store.store
    sql = raw"""
        SELECT * FROM entityrelations
        WHERE domain_id = $1
        ORDER BY name;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (domain_id,)) |> DataFrame
    return df
end
"""
    list_assets_df(store::DataStore, study::Study; include_versions=true)::DataFrame

Return a DataFrame containing all assets in the specified study.
- 'store' is the DataStore object containing the database connection.
- 'study' is the Study object to list assets from.
- 'include_versions' is a boolean flag indicating whether to include asset versions in the DataFrame.
If `include_versions` is true, the DataFrame will include asset version details
"""
function list_assets_df(store::DataStore, study::Study; include_versions=true)::DataFrame
    sql = ""
    if include_versions
        sql = raw"""
            SELECT 
                a.asset_id, a.name, a.description, a.asset_type,
                a.date_created, a.created_by,
                v.version_id,
                v.version_label,
                v.version_note,
                v.created_at version_created,
                v.created_by version_created_by
            FROM public.assets a
              JOIN public.asset_versions v USING(asset_id)
            WHERE v.is_latest = true
              AND a.study_id = $1
            ORDER BY a.name;
        """
    else
        sql = raw"""
            SELECT asset_id, name, description, asset_type
              FROM assets
             WHERE study_id = $1
             ORDER BY name;
        """
    end
    return DBInterface.execute(DBInterface.prepare(store.store, sql), (study.study_id,)) |> DataFrame
end
"""
    list_assets(store::DataStore, study::Study; include_versions=true)::Vector{Asset}
Return a DataFrame containing all assets in the specified study.
- 'store' is the DataStore object containing the database connection.
- 'study' is the Study object to list assets from.
- 'include_versions' is a boolean flag indicating whether to include asset versions in the returned Asset objects.
The DataFrame will contain all columns from the assets table, ordered by name.
"""
function list_assets(store::DataStore, study::Study; include_versions=true)::Vector{Asset}
    df = list_assets_df(store, study, include_versions=false)
    if nrow(df) == 0
        return Asset[]
    end
    assets = Vector{Asset}(undef, nrow(df))
    for i in 1:nrow(df)
        row = df[i, :]
        asset = Asset(
            asset_id=UUID(row.asset_id),
            study=study,
            name=row.name,
            description=coalesce(row.description, missing),
            asset_type=row.asset_type
        )
        if include_versions
            asset.versions = get_assetversions(store, asset)
        else
            asset.versions = AssetVersion[]
        end
        assets[i] = asset
    end
    return assets
end
"""
    get_asset(store::DataStore, study::Study, name::String)::Union{Asset,Nothing}

    Return an Asset object by its name in the specified study.
    - 'store' is the DataStore object containing the database connection.
    - 'study' is the Study object to search in.
    - 'name' is the name of the asset to search for.
    If no asset is found, it returns `nothing`.
    If an asset is found, it returns an Asset object 
"""
function get_asset(store::DataStore, study::Study, name::String; include_versions=true, asset_type::Union{Nothing,String}=nothing)::Union{Asset,Nothing}
    conn = store.store
    df = nothing
    if !isnothing(asset_type)
        sql = raw"""
            SELECT * FROM assets
            WHERE study_id = $1 AND name = $2 AND asset_type = $3
            LIMIT 1;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql), (study.study_id, name, asset_type)) |> DataFrame
    else
        sql = raw"""
            SELECT * FROM assets
            WHERE study_id = $1 AND name = $2
            LIMIT 1;
        """
        df = DBInterface.execute(DBInterface.prepare(conn, sql), (study.study_id, name)) |> DataFrame
    end
    if nrow(df) == 0
        return nothing
    end
    asset = make_asset(store, df[1, :], study, include_versions=include_versions)
    return asset
end
"""
    make_asset(row::DataFrameRow, study::Study; include_versions=true)::Asset

Helper function to create an Asset object from a DataFrameRow and a Study.
- `store` is the DataStore object containing the database connection.
- `row` is a DataFrameRow containing asset data.
- `study` is the Study object to associate with the asset.
- `include_versions` is a boolean flag indicating whether to include asset versions in the Asset object.
This function returns an Asset object with its versions populated if requested.
"""
function make_asset(store::DataStore, row::DataFrameRow, study::Study; include_versions=true)::Asset
    asset = Asset(
        asset_id=UUID(row.asset_id),
        study=study,
        name=row.name,
        description=coalesce(row.description, missing),
        asset_type=row.asset_type
    )
    if include_versions
        # Populate asset versions if requested
        asset.versions = get_assetversions(store, asset)
    else
        asset.versions = AssetVersion[]
    end
    return asset
end
"""
    get_asset(store::DataStore, asset_id::UUID)::Union{Asset,Nothing}

Return an Asset object by its asset_id in the specified DataStore.
- 'store' is the DataStore object containing the database connection.
- 'asset_id' is the UUID of the asset to search for.
If no asset is found, it returns `nothing`.
If an asset is found, it returns an Asset object with its versions populated.
"""
function get_asset(store::DataStore, asset_id::UUID; include_versions=true)::Union{Asset,Nothing}
    conn = store.store
    sql = raw"""
        SELECT asset_id, study_id, name, description, asset_type
          FROM assets
         WHERE asset_id = $1
         LIMIT 1;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (asset_id,)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    study = get_study(store, UUID(row.study_id))
    asset = make_asset(store, row, study, include_versions=include_versions)
    return asset
end
"""
    get_assetversions(store::DataStore, asset::Asset)::Vector{AssetVersion}

Return a vector of AssetVersion objects for the specified asset.
- 'store' is the DataStore object containing the database connection.
- 'asset' is the Asset object for which to retrieve versions.
If no versions are found, it returns an empty vector.
"""
function get_assetversions(store::DataStore, asset::Asset)::Vector{AssetVersion}
    conn = store.store
    sql = raw"""
        SELECT version_id, asset_id, major, minor, patch, version_note, is_latest, doi
        FROM asset_versions
        WHERE asset_id = $1
        ORDER BY major, minor, patch;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (asset.asset_id,)) |> DataFrame
    if nrow(df) == 0
        return AssetVersion[]
    end
    versions = Vector{AssetVersion}(undef, nrow(df))
    for i in 1:nrow(df)
        row = df[i, :]
        versions[i] = AssetVersion(
            version_id=UUID(row.version_id),
            asset=asset,
            major=row.major,
            minor=row.minor,
            patch=row.patch,
            note=coalesce(row.version_note, missing),
            doi=coalesce(row.doi, missing),
            is_latest=row.is_latest
        )
    end
    return versions
end
"""
    get_latest_version(asset::Asset)::Union{AssetVersion,Nothing}

Return the latest AssetVersion for the specified asset.
- 'asset' is the Asset object for which to retrieve the latest version.
"""
function get_latest_version(asset::Asset)::Union{AssetVersion,Nothing}
    # If there are no versions, retrieve them
    if isempty(asset.versions)
        asset.versions = get_assetversions(store, asset)
    end
    # Find the version flagged as latest
    idx = findfirst(v -> v.is_latest, asset.versions)
    if idx === nothing
        # Fallback: if no version is flagged as latest, return the last version
        return asset.versions[end]
    end
    return asset.versions[idx]
end

"""
    get_latest_version(store::DataStore, asset_id::UUID)::Union{AssetVersion,Nothing}

Return the latest AssetVersion for the specified asset_id in the DataStore.
- 'store' is the DataStore object containing the database connection.
- 'asset_id' is the UUID of the asset for which to retrieve the latest version.
If no latest version is found, it returns `nothing`.
"""
function get_latest_version(store::DataStore, asset_id::UUID)::Union{AssetVersion,Nothing}
    conn = store.store
    sql = raw"""
        SELECT version_id, asset_id, major, minor, patch, version_note, is_latest, doi
        FROM asset_versions
        WHERE asset_id = $1 AND is_latest = true
        LIMIT 1;
    """
    df = DBInterface.execute(DBInterface.prepare(conn, sql), (asset_id,)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    asset = get_asset(store, UUID(row.asset_id); include_versions=false)
    return AssetVersion(
        version_id=row.version_id,
        asset=asset,
        major=row.major,
        minor=row.minor,
        patch=row.patch,
        note=coalesce(row.version_note, missing),
        doi=coalesce(row.doi, missing),
        is_latest=row.is_latest
    )
end
"""
    create_asset(store::DataStore, study::Study, name::String, type::String, description::Union{Missing,String}=missing)::Asset

Create a new asset in the TRE datastore and the base version of the asset.
- `store`: The DataStore object containing connection details for the datastore.
- `study`: The Study object to associate with the asset.
- `name`: The name of the asset. Will be coherced to xsd:NCName format.
- `type`: The type of the asset, either "dataset" or "file".
- `description`: An optional description of the asset (default is missing).
Returns the created Asset object with its asset_id and the first version.
"""
function create_asset(store::DataStore, study::Study, name::String, type::String, description::Union{Missing,String}=missing)::Asset
    if !is_ncname(name)
        @info "Asset name '$name' must be a valid xsd:NCName format"
        name = to_ncname(name)
        @info "Coerced asset name to '$name'"
    end
    asset = Asset(study=study, name=name, description=description, asset_type=type)
    db = store.store
    sql = raw"""
        INSERT INTO assets (study_id, name, description, asset_type)
        VALUES ($1, $2, $3, $4)
        RETURNING asset_id;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (asset.study.study_id, asset.name, asset.description, asset.asset_type)) |> DataFrame
    asset.asset_id = UUID(df[1, :asset_id])
    # Create the first version of the asset
    version = AssetVersion(asset=asset, major=1, minor=0, patch=0, note="Initial version", is_latest=true)
    save_version!(store, version)
    push!(asset.versions, version) # Add the version to the asset's versions
    return asset
end
function save_version!(store::DataStore, version::AssetVersion)::AssetVersion
    db = store.store
    if version.is_latest
        # If this version is marked as latest, unset the is_latest flag on all other versions of the asset
        sql_unset = raw"""
            UPDATE asset_versions
               SET is_latest = false
             WHERE asset_id = $1 AND is_latest = true;
        """
        stmt_unset = DBInterface.prepare(db, sql_unset)
        DBInterface.execute(stmt_unset, (version.asset.asset_id,))
    end
    sql = raw"""
        INSERT INTO asset_versions (asset_id, major, minor, patch, version_note, is_latest, doi)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING version_id;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (version.asset.asset_id, version.major, version.minor,
        version.patch, version.note, version.is_latest, version.doi)) |> DataFrame
    version.version_id = UUID(df[1, :version_id])
    return version
end
"""
    set_version(assetverion::AssetVersion, major::Int, minor::Int, patch::Int)::AssetVersion

Set the version numbers of an AssetVersion object.
- `assetversion`: The AssetVersion object to update.    
- `major`: The major version number.
- `minor`: The minor version number.
- `patch`: The patch version number.
Returns the updated AssetVersion object.
"""
function set_version(assetverion::AssetVersion, major::Int, minor::Int, patch::Int)::AssetVersion
    assetversion.major = major
    assetversion.minor = minor
    assetversion.patch = patch
    return assetversion
end
"""
    set_version(assetverion::AssetVersion, version::VersionNumber)::AssetVersion

Set the version numbers of an AssetVersion object using a VersionNumber object.
- `assetversion`: The AssetVersion object to update.    
- `version`: The VersionNumber object containing major, minor, and patch numbers.
Returns the updated AssetVersion object.
"""
function set_version(assetverion::AssetVersion, version::VersionNumber)::AssetVersion
    return set_version(assetverion, version.major, version.minor, version.patch)
end
"""
    save_asset_version!(store::DataStore, existing_asset::Asset, description::String, new_version::Union{VersionNumber,Nothing})

Save a new version of an existing asset in the TRE datastore.
- `store`: The DataStore object containing connection details for the datastore.
- `existing_asset`: The existing Asset object for which to create a new version.
- `description`: A description or note for the new version.
- `new_version`: An optional VersionNumber object specifying the new version numbers. If `nothing`, the patch number of the latest version will be incremented by 1.
This function will create and save a new AssetVersion for the existing asset.
"""
function save_asset_version!(store::DataStore, existing_asset::Asset, description::String, new_version::Union{VersionNumber,Nothing})
    if isnothing(new_version)
        latest_version = get_latest_version(existing_asset)
        if isnothing(latest_version)
            error("No existing versions found for asset $(existing_asset.name)")
        end
        new_version = VersionNumber(latest_version.major, latest_version.minor, latest_version.patch + 1)
    end
    asset_version = AssetVersion(
        asset=existing_asset,
        major=new_version.major,
        minor=new_version.minor,
        patch=new_version.patch,
        note=description,
        is_latest=true
    )
    for version in existing_asset.versions
        version.is_latest = false
    end
    saved_version = save_version!(store, asset_version)
    push!(existing_asset.versions, saved_version)
    return nothing
end
"""
    register_datafile(store::DataStore, datafile::DataFile)

Register a DataFile in the TRE datastore. The assetversion must already exist in the datastore.
- `store`: The Datastore object containing connection details for the datastore.
- `datafile`: The DataFile object to register, which must have a valid assetversion.
This function will insert the datafile into the database and associate it with the specified asset version.
"""
function register_datafile(store::DataStore, datafile::DataFile)
    db = store.store
    try
        sql = raw"""
            INSERT INTO datafiles (datafile_id, compressed, encrypted, compression_algorithm, storage_uri, edam_format, digest)
            VALUES ($1, $2, $3, $4, $5, $6, $7);
        """
        stmt = DBInterface.prepare(db, sql)
        DBInterface.execute(stmt, (datafile.version.version_id, datafile.compressed, datafile.encrypted,
            datafile.compression_algorithm, datafile.storage_uri, datafile.edam_format, datafile.digest))
    catch e
        @error "Failed to register datafile: $e"
        rethrow(e)
    end
    return nothing
end
"""
    register_dataset(store::DataStore, dataset::DataSet)::Nothing

Register a Dataset in the TRE datastore.
- `store`: The DataStore object containing connection details for the datastore.
- `dataset`: The Dataset object to register, which must have a valid dataset_id.
This function will insert the dataset into the database.
"""
function register_dataset(store::DataStore, dataset::DataSet)::Nothing
    db = store.store
    sql = raw"""
        INSERT INTO datasets (dataset_id)
        VALUES ($1);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (dataset.version.version_id,))
    return nothing
end
"""
    get_datafile_meta(store::DataStore, assetversion::AssetVersion)::Union{DataFile,Nothing}

Get metadata for a DataFile associated with the specified AssetVersion.
- `store`: The DataStore object containing connection details for the datastore.
- `assetversion`: The AssetVersion object for which to retrieve the DataFile metadata.
If no DataFile is found, it returns `nothing`.
"""
function get_datafile_meta(store::DataStore, assetversion::AssetVersion)::Union{DataFile,Nothing}
    if assetversion.version_id === nothing
        @error "AssetVersion must have a valid version_id to retrieve DataFile metadata"
        return nothing
    end
    if assetversion.asset.asset_type != "file"
        @error "AssetVersion must be of type 'file' to have associated DataFile metadata"
        return nothing
    end
    # Query the datafiles table for the given asset version
    db = store.store
    sql = raw"""
        SELECT datafile_id, compressed, encrypted, compression_algorithm, storage_uri, edam_format, digest
          FROM datafiles
         WHERE datafile_id = $1
         LIMIT 1;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (assetversion.version_id,)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    return DataFile(
        version=assetversion,
        compressed=row.compressed,
        encrypted=row.encrypted,
        compression_algorithm=row.compression_algorithm,
        storage_uri=row.storage_uri,
        edam_format=row.edam_format,
        digest=row.digest
    )
end
"""
    save_transformation!(store::DataStore, transformation::Transformation)::Transformation

Save a transformation in the TRE datastore.
- `store`: The DataStore object containing connection details for the datastore.
- `transformation`: The Transformation object to save
This function will insert the transformation into the database and return the updated Transformation object with its transformation_id set.
"""
function save_transformation!(store::DataStore, transformation::Transformation)::Transformation
    db = store.store
    @info "Inserting new transformation: $(transformation.description)"
    if !isnothing(transformation.transformation_id)
        error("Transformation ID must be nothing when saving a new transformation")
    end
    sql = raw"""
        INSERT INTO transformations (transformation_type, description, repository_url, commit_hash, file_path)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING transformation_id;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (transformation.transformation_type, transformation.description,
        transformation.repository_url, transformation.commit_hash, transformation.file_path)) |> DataFrame
    transformation.transformation_id = df[1, :transformation_id]
    return transformation
end
"""
    add_transformation_output(store::DataStore, transformation_id::Int, version_id::UUID)::Nothing

Add a transformation output to the transformation_outputs table.
- `store`: The DataStore object containing connection details for the datastore.
- `transformation_id`: The ID of the transformation to which the output belongs.
- `version_id`: The UUID of the asset version that is the output of the transformation.
This function will insert a new record into the transformation_outputs table linking the transformation to the asset version.
"""
function add_transformation_output(store::DataStore, transformation_id::Int, version_id::UUID)::Nothing
    db = store.store
    @info "Adding transformation output for transformation ID $(transformation_id) and version ID $(version_id)"
    sql = raw"""
        INSERT INTO transformation_outputs (transformation_id, version_id)
        VALUES ($1, $2);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (transformation_id, version_id))
    return nothing
end
"""
    add_transformation_input(store::DataStore, transformation_id::Int, version_id::UUID)::Nothing

Add a transformation input to the transformation_inputs table.
- `store`: The DataStore object containing connection details for the datastore.
- `transformation_id`: The ID of the transformation to which the input belongs.
- `version_id`: The UUID of the asset version that is the input to the transformation.
This function will insert a new record into the transformation_inputs table linking the transformation to the asset version.
"""
function add_transformation_input(store::DataStore, transformation_id::Int, version_id::UUID)::Nothing
    db = store.store
    @info "Adding transformation input for transformation ID $(transformation_id) and version ID $(version_id)"
    sql = raw"""
        INSERT INTO transformation_inputs (transformation_id, version_id)
        VALUES ($1, $2);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (transformation_id, version_id))
    return nothing
end
"""
    add_transformation_output(store::DataStore, transformation::Transformation, version::AssetVersion)::Nothing

Add a transformation output to the transformation_outputs table using Transformation and AssetVersion objects.
- `store`: The DataStore object containing connection details for the datastore.
- `transformation`: The Transformation object containing the transformation_id.
- `version`: The AssetVersion object containing the version_id.
This function checks that both the transformation and version have valid IDs before calling the lower-level function.
"""
function add_transformation_output(store::DataStore, transformation::Transformation, version::AssetVersion)::Nothing
    if transformation.transformation_id === nothing
        @error "Transformation must have a valid transformation_id to add an output"
        return nothing
    end
    if version.version_id === nothing
        @error "AssetVersion must have a valid version_id to add as a transformation output"
        return nothing
    end
    add_transformation_output(store, transformation.transformation_id, version.version_id)
end
"""
    add_transformation_input(store::DataStore, transformation::Transformation, version::AssetVersion)::Nothing

Add a transformation input to the transformation_inputs table using Transformation and AssetVersion objects.
- `store`: The DataStore object containing connection details for the datastore.
- `transformation`: The Transformation object containing the transformation_id.
- `version`: The AssetVersion object containing the version_id.
This function checks that both the transformation and version have valid IDs before calling the lower-level function.
"""
function add_transformation_input(store::DataStore, transformation::Transformation, version::AssetVersion)::Nothing
    if transformation.transformation_id === nothing
        @error "Transformation must have a valid transformation_id to add an input"
        return nothing
    end
    if version.version_id === nothing
        @error "AssetVersion must have a valid version_id to add as a transformation input"
        return nothing
    end
    add_transformation_input(store, transformation.transformation_id, version.version_id)
end
"""
    list_study_transformations(store::DataStore, study::Study)::DataFrame

Return a DataFrame containing all transformations associated with assets in the specified study.
- 'store' is the DataStore object containing the database connection.
- 'study' is the Study object to list transformations from.
The DataFrame will contain all columns from the transformations table, ordered by date_created descending.
"""
function list_study_transformations(store::DataStore, study::Study)::DataFrame
    db = store.store
    sql = raw"""
    with linked_assets as (
        select distinct av.asset_id, ti.transformation_id
        from public.asset_versions av 
            join public.transformation_inputs ti on av.version_id = ti.version_id
            join public.assets a on av.asset_id = a.asset_id
        where a.study_id = $1
        union 
        select distinct av.asset_id, ti.transformation_id
        from public.asset_versions av 
            join public.transformation_outputs ti on av.version_id = ti.version_id
            join public.assets a on av.asset_id = a.asset_id
        where a.study_id = $1
    )
    select distinct t.* from public.transformations t
    join linked_assets l on t.transformation_id = l.transformation_id
    order by t.date_created desc;
"""
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (study.study_id,)) |> DataFrame
end
"""
    get_eav_variables(store::DataStore, datafile::DataFile)::Vector{Variable}

Return a vector of Variable objects representing the EAV variables in the specified DataFile.
- 'store' is the DataStore object containing the database connection.
- 'datafile' is the DataFile object for which to retrieve EAV variables.

"""
function get_eav_variables(store::DataStore, datafile::DataFile)::DataFrame
    db = store.lake
    sql = """
    SELECT DISTINCT field_name 
    FROM '$(file_uri_to_path(datafile.storage_uri))'
    ORDER BY field_name;    
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt) |> DataFrame
end
function get_study_variables(store::DataStore, study::Study)::DataFrame
    db = store.store
    sql = raw"""
        SELECT
          v.*
        FROM public.variables v
        JOIN public.domains d ON v.domain_id = d.domain_id
        JOIN public.study_domains s ON s.domain_id = d.domain_id
        WHERE s.study_id = $1;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (study.study_id,)) |> DataFrame
end
"""
    get_vocabulary(store::DataStore, vocabulary_id::Int)::Union{Vocabulary,Nothing}

Return a Vocabulary object by its vocabulary_id in the specified DataStore.
- 'store' is the DataStore object containing the database connection.
- 'vocabulary_id' is the ID of the vocabulary to retrieve.
If no vocabulary is found, it returns `nothing`.
If a vocabulary is found, it returns a Vocabulary object with its items populated.
"""
function get_vocabulary(store::DataStore, vocabulary_id::Int)::Union{Vocabulary,Nothing}
    db = store.store
    sql = raw"""
        SELECT v.*, 
          vi.vocabulary_item_id,
          vi.value,
          vi.code,
          vi.description AS item_description
        FROM public.vocabularies v
        LEFT JOIN public.vocabulary_items vi ON vi.vocabulary_id = v.vocabulary_id
        WHERE v.vocabulary_id = $1;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (vocabulary_id,)) |> DataFrame
    if nrow(df) == 0
        return nothing
    end
    row = df[1, :]
    vocabulary = Vocabulary(
        vocabulary_id=vocabulary_id,
        name=row.name,
        description=coalesce(row.description, missing)
    )
    for row in eachrow(df)
        if !ismissing(row.value)
            item = VocabularyItem(
                vocabulary_item_id=row.vocabulary_item_id,
                vocabulary_id=vocabulary_id,
                value=row.value,
                code=row.code,
                description=coalesce(row.item_description, missing)
            )
            push!(vocabulary.items, item)
        end
    end
    return vocabulary
end

"""
    ensure_vocabulary!(db, vocab::Vocabulary) -> Int

Ensure that `vocab` exists in the datastore `vocabularies` table and (re)load its items into
`vocabulary_items`.

Returns the `vocabulary_id`.
"""
function ensure_vocabulary!(db, vocab::Vocabulary)::Int
    # Delegate to the string-based helper (defined in redcap.jl) to keep behaviour consistent.
    desc = coalesce(vocab.description, "")
    return ensure_vocabulary!(db, vocab.name, desc, vocab.items)
end

"""
    save_variables!(store::DataStore, dataset::DataSet)::Nothing

Persist the variable metadata attached to `dataset.variables` into the datastore metadata tables:

- Upserts rows in `variables` (unique on `(domain_id, name)`)
- Creates/updates vocabularies and vocabulary_items for categorical/multiresponse variables
- Links the dataset version to its variables in `dataset_variables`

On return, each `Variable` in `dataset.variables` will have `variable_id` (and where applicable
`vocabulary_id`) populated.
"""
function save_variables!(store::DataStore, dataset::DataSet)::Nothing
    db = store.store
    if isnothing(db)
        throw(ArgumentError("No datastore connection available in store.store"))
    end
    if isnothing(dataset.version) || isnothing(dataset.version.version_id)
        throw(ArgumentError("Dataset must have a persisted version (dataset.version.version_id)"))
    end

    for var in dataset.variables
        # Resolve vocabulary_id if this variable carries a vocabulary payload.
        vocab_id = missing
        if var.value_type_id in (TRE_TYPE_CATEGORY, TRE_TYPE_MULTIRESPONSE)
            if !ismissing(var.vocabulary)
                vocab_id = ensure_vocabulary!(db, var.vocabulary)
                var.vocabulary_id = Int(vocab_id)
                var.vocabulary.vocabulary_id = Int(vocab_id)
            elseif !(var.vocabulary_id === missing)
                vocab_id = Int(var.vocabulary_id)
            end
        end

        value_format = (var.value_format === nothing) ? missing : var.value_format
        vocabulary_arg = (vocab_id === missing) ? missing : Int(vocab_id)

        # Upsert variable record and capture variable_id.
        variable_id = upsert_variable!(
            db,
            var.domain_id,
            var.name;
            value_type_id=var.value_type_id,
            value_format=value_format,
            vocabulary_id=vocabulary_arg,
            description=var.description,
            note=missing,
            keyrole=var.keyrole
        )
        var.variable_id = Int(variable_id)
    end

    # Link dataset to its variables
    register_dataset_variables(store, dataset)
    return nothing
end

function register_dataset_variables(store::DataStore, dataset::DataSet)::Nothing
    db = store.store
    sql = raw"""
        INSERT INTO dataset_variables (dataset_id, variable_id, keyrole)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING;
    """
    stmt = DBInterface.prepare(db, sql)
    for variable in dataset.variables
        DBInterface.execute(stmt, (dataset.version.version_id, variable.variable_id, variable.keyrole))
    end
    return nothing
end
"""
    create_dataset_meta(store::DataStore, study::Study, dataset_name::String, description::String, datafile::DataFile)::DataSet

Create a new dataset metadata object from an EAV data file.
- 'store' is the DataStore object containing the database connection.
- 'study' is the Study object to associate with the dataset.
- 'dataset_name' is the name of the dataset to create.
- 'description' is a description for the dataset.
- 'datafile' is the DataFile object representing the EAV data file that the dataset is derived from.
This function creates a new dataset asset and version, collects variable metadata from the EAV data file,
and returns a DataSet object containing the dataset metadata.
It assumes the EAV data is stored in a csv table with columns: record, field_name
"""
function create_dataset_meta(store::DataStore, study::Study, dataset_name::String, description::String, datafile::DataFile)::DataSet
    # Create the dataset asset and version
    new_asset = create_asset(store, study, dataset_name, "dataset", description)
    dataset = DataSet(version=get_latest_version(new_asset))
    register_dataset(store, dataset)
    # Collect variable metadata and EAV fields, then join
    variables = get_study_variables(store, study)
    eav_variables = get_eav_variables(store, datafile)
    variables = leftjoin(eav_variables, variables, on=:field_name => :name, makeunique=true)
    @info "Variables = $(names(variables))"
    @info "Found $(nrow(variables)) variables in EAV data for dataset $(dataset_name)"
    for row in eachrow(variables)
        @info "Processing variable: $(row.field_name) with ID $(row.variable_id)"
        if ismissing(row.variable_id)
            @warn "Skipping variable $(row.field_name) with missing name in EAV data"
            continue
        end
        variable = Variable(
            variable_id=row.variable_id,
            domain_id=row.domain_id,
            name=row.field_name,
            value_type_id=row.value_type_id,
            vocabulary_id=coalesce(row.vocabulary_id, missing),
            keyrole=row.keyrole,
            description=coalesce(row.description, missing),
            ontology_namespace=coalesce(row.ontology_namespace, missing),
            ontology_class=coalesce(row.ontology_class, missing),
        )
        if variable.value_type_id in (TRE_TYPE_CATEGORY, TRE_TYPE_MULTIRESPONSE)
            variable.vocabulary = get_vocabulary(store, variable.vocabulary_id)
        end
        push!(dataset.variables, variable)
    end
    register_dataset_variables(store, dataset)
    return dataset
end
"""
    quote_ident(name::AbstractString)

Return a quoted identifier for SQL queries.
- 'name' is the identifier to quote, typically a table or column name.
This function wraps the identifier in double quotes and escapes any existing double quotes by doubling them.
"""
quote_ident(name::AbstractString) = "\"" * replace(name, "\"" => "\"\"") * "\""
"""
    quote_sql_str(s::AbstractString)

Return a quoted SQL string for use in SQL queries.
- 's' is the string to quote.
This function wraps the string in single quotes and escapes any existing single quotes by doubling them.
"""
quote_sql_str(s::AbstractString) = "'" * replace(s, "'" => "''") * "'"
"""
    transform_eav_to_table!(store::DataStore, datafile::DataFile, dataset::DataSet)::Nothing

Transform an EAV (Entity-Attribute-Value) data file into a table in the TRE lake.
- 'store' is the DataStore object containing the database connection.
- 'datafile' is the DataFile object representing the EAV data file.
- 'dataset' is the DataSet object representing the target dataset.
- 'convert' is a boolean flag indicating whether to convert the output table to the value_types defined in the dataset.
This function creates a new table in the TRE lake by pivoting the EAV data into a wide format.
It aggregates multiple values for the same field per record into a single column.
The table name is derived from the dataset's asset name.
This function assumes the EAV data is stored in a csv table with columns: record, field_name, and value.
"""
function transform_eav_to_table!(store::DataStore, datafile::DataFile, dataset::DataSet; convert=true)::Nothing
    tbl = LAKE_ALIAS * "." * get_datasetname(dataset, include_schema=true)

    schema = to_ncname(datafile.version.asset.study.name)
    sql = "CREATE SCHEMA IF NOT EXISTS $(schema);"
    DuckDB.query(store.lake, sql)

    fpath = quote_sql_str(file_uri_to_path(datafile.storage_uri))
    sql = """
    CREATE OR REPLACE TABLE $(tbl) AS
    SELECT * FROM (
        WITH src AS (
            SELECT * FROM read_csv_auto($(fpath))
        ),
        counts AS (
            SELECT record, field_name, COUNT(*) AS n
            FROM src
            GROUP BY record, field_name
        ),
        multi_fields AS (
            SELECT DISTINCT field_name
            FROM counts
            WHERE n > 1
        ),
        single_src AS (
            SELECT s.*
            FROM src s
            LEFT JOIN multi_fields m USING (field_name)
            WHERE m.field_name IS NULL
        ),
        multi_src AS (
            SELECT record, field_name, string_agg("value", ', ') AS value
            FROM src
            WHERE field_name IN (SELECT field_name FROM multi_fields)
            GROUP BY record, field_name
        ),
        prepped AS (
            SELECT record, field_name, value FROM single_src
            UNION ALL
            SELECT record, field_name, value FROM multi_src
        ),
        pivoted AS (
            PIVOT prepped
            ON field_name
            USING any_value(value)   -- or first(value)
            GROUP BY record
        )
    """
    sql2 = ""
    if !isempty(dataset.variables) && convert
        sql2 = "SELECT CAST(record AS INTEGER) AS record"
        for variable in dataset.variables
            col = quote_ident(variable.name)
            if variable.value_type_id == TRE_TYPE_CATEGORY
                sql2 *= ", TRY_CAST($col AS INTEGER) AS $col, $col AS $(quote_ident(variable.name * "_raw"))"
            elseif variable.value_type_id == TRE_TYPE_INTEGER
                sql2 *= ", CAST($col AS INTEGER) AS $col"
            elseif variable.value_type_id == TRE_TYPE_FLOAT
                sql2 *= ", CAST($col AS DOUBLE) AS $col"
            elseif variable.value_type_id == TRE_TYPE_DATE
                sql2 *= ", CAST(strptime($col, '$(variable.value_format)') AS DATE) AS $col"
            elseif variable.value_type_id == TRE_TYPE_DATETIME
                sql2 *= ", strptime($col, '$(variable.value_format)') AS $col"
            elseif variable.value_type_id == TRE_TYPE_TIME
                sql2 *= ", CAST(strptime($col, '$(variable.value_format)') AS TIME) AS $col"
            else
                sql2 *= ", $col"
            end
        end
        sql2 *= """
            FROM pivoted
            ORDER BY record
        ) t;
        """
    else
        sql2 = """
            SELECT * FROM pivoted
            ORDER BY record
        ) t;
        """
    end
    @info "Conversion sql:\n $(sql * sql2)"
    DuckDB.query(store.lake, sql * sql2)
    @info "Transformed EAV data from $(fpath) to table $(tbl)"
    return nothing
end
"""
    dataset_to_dataframe(store::DataStore, dataset::DataSet)::DataFrame

Retrieve a dataset from store.lake and return as a DataFrame
- `store`: The DataStore object containing the datastore and datalake connections.
- `dataset`: The DataSet object to be retrieved. NB version must be set.
"""
function dataset_to_dataframe(store::DataStore, dataset::DataSet)::DataFrame
    table = get_datasetname(dataset, include_schema=true)
    sql = "SELECT * FROM $LAKE_ALIAS.$(table)"
    return DuckDB.query(store.lake, sql) |> DataFrame
end
# Map TRE variable type -> DuckDB SQL type
"""
    tre_type_to_duckdb_sql(value_type_id::Int)::String

Map TRE variable type IDs to DuckDB SQL data types.
- 'value_type_id' is the TRE variable type ID. 
This function returns the corresponding DuckDB SQL data type as a string.
"""
function tre_type_to_duckdb_sql(value_type_id::Int)::String
    if value_type_id == TRE_TYPE_INTEGER
        return "INTEGER"
    elseif value_type_id == TRE_TYPE_FLOAT
        return "DOUBLE"
    elseif value_type_id == TRE_TYPE_DATE
        return "DATE"
    elseif value_type_id == TRE_TYPE_TIME
        return "TIME"
    elseif value_type_id == TRE_TYPE_DATETIME
        return "TIMESTAMP"
    elseif value_type_id == TRE_TYPE_CATEGORY
        # Store category values as BIGINT (FK/code) by default; adjust if you store codes instead
        return "SMALLINT"
    else
        return "VARCHAR"
    end
end

"""
    create_duckdb_table_sql(table_name::AbstractString, variables::Vector{Variable})::String

Generate a CREATE TABLE SQL statement for DuckDB based on the provided variables.
- 'table_name' is the name of the table to create.
- 'variables' is a vector of Variable objects defining the table schema.
This function returns a SQL string that can be executed to create the table in DuckDB.
"""
function create_duckdb_table_sql(table_name::AbstractString, variables::Vector{Variable})::String
    cols = String[]
    for v in variables
        colname = replace(v.name, "\"" => "\"\"") # minimal escaping
        push!(cols, "\"$colname\" $(tre_type_to_duckdb_sql(v.value_type_id))")
    end
    ident = quote_qualified_identifier(table_name)
    return "CREATE TABLE $(ident) (\n  $(join(cols, ",\n  "))\n)"
end

function quote_qualified_identifier(name::AbstractString)::String
    parts = split(name, '.')
    quoted_parts = ["\"" * replace(part, "\"" => "\"\"") * "\"" for part in parts]
    return join(quoted_parts, ".")
end

"""
    load_query(datastore::DataStore, dataset::DataSet, source_conn::DBInterface.Connection, sql::AbstractString)

Load data from a source database connection into a dataset table in the TRE lake.
- 'datastore' is the DataStore object containing the datastore and datalake connections.
- 'dataset' is the DataSet object representing the target dataset.
- 'source_conn' is the DBInterface.Connection object for the source database.
- 'sql' is the SQL query string to retrieve data from the source database.
This function creates the target table in the TRE lake based on the dataset's variables
"""
function load_query(datastore::DataStore, dataset::DataSet, source_conn::DBInterface.Connection, sql::AbstractString)
    schema = to_ncname(dataset.version.asset.study.name)
    DuckDB.query(datastore.lake, "CREATE SCHEMA IF NOT EXISTS $(schema);")
    table = get_datasetname(dataset, include_schema=false)
    DBInterface.execute(datastore.lake, create_duckdb_table_sql(schema * "." * table, dataset.variables))
    app = DuckDB.Appender(datastore.lake, table, schema)
    try
        res = DBInterface.execute(source_conn, sql)

        # Iterate rows without DataFrames
        for row in Tables.rows(res)
            # Build a row in the same column order as `variables`
            vals = Any[]
            for v in dataset.variables
                sym = Symbol(v.name)
                push!(vals, getproperty(row, sym))
            end
            for v in vals
                DuckDB.append(app, v)
            end
            DuckDB.end_row(app)
        end
        DuckDB.flush(app)
    finally
        DuckDB.close(app)
    end

    return nothing
end