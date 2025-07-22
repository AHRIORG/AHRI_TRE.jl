"""
    createdatabase(path, name; replace=false, sqlite=true)

Creates a database to store the information contained in the Trusted Research Environment (TRE)
By default a sqlite database is created, but this can be changed by setting the sqlite argument to false, 
in which case a sql server database is created and path is interpreted as the name of the database server.
"""
function createdatabase(path, name; replace=false, sqlite=true)
    if sqlite
        db = createdatabasesqlite(path, name; replace=replace)
    else
        db = createdatabasesqlserver(path, name; replace=replace)
    end
    try
        createstudies(db)
        createtransformations(db)
        createvariables(db)
        createdatasets(db)
        createentities(db)
        createmapping(db)
        return nothing
    finally
        DBInterface.close!(db)
    end
end
"""
    createdatabasesqlite(path, name; replace=replace)::SQLite.DB

Create an sqlite database on path with name, if replace = true then replace any existing database
"""
function createdatabasesqlite(path, name; replace=replace)::SQLite.DB
    file = joinpath(path, "$name.sqlite")
    existed = isfile(file)
    if existed && !replace
        error("Database '$file' already exists.")
    end
    if existed && replace
        GC.gc() #to ensure database file is released
        rm(file)
    end
    if !existed && !isdir(path)
        mkpath(path)
    end
    return SQLite.DB(file)
end
"""
    createdatabasesqlserver(server, name; replace=replace)::ODBC.Connection

Create a SQL Server database on server with name, if replace = true then replace any existing database
"""
function createdatabasesqlserver(server, name; replace=replace)::ODBC.Connection
    master = ODBC.Connection("Driver=ODBC Driver 17 for SQL Server;Server=$server;Database=master;Trusted_Connection=yes;")
    if replace
        sql = """
            USE master;  -- Switch to the master database to perform the operations
            -- Check if the database exists
            IF EXISTS (SELECT name FROM sys.databases WHERE name = '$name')
            BEGIN
                -- Close all active connections
                ALTER DATABASE $name SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                -- Drop the database
                DROP DATABASE $name;
            END
        """
        DBInterface.execute(master, sql)
    end
    sql = "CREATE DATABASE $name"
    DBInterface.execute(master, sql)
    DBInterface.close!(master)
    return ODBC.Connection("Driver=ODBC Driver 17 for SQL Server;Server=$server;Database=$name;Trusted_Connection=yes;")
end
"""
    opendatabase(path::String, name::String; sqlite=true, lake_data::String=nothing, lake_db::String=nothing)

If sqlite = true (default) open file on path as an SQLite database (assume .sqlite extension)
else open database 'name' on server 'path' (assume SQL Server database)
    lake_data and lake_db are optional parameters to open a ducklake connection in which to store datasets
    lake_data is the path to the ducklake data directory, and lake_db is the name of the ducklake metadata database file (without extension)
Returns a tuple of the SQLite database connection and the DuckDB connection if lake_data and lake_db are provided.
"""
function opendatabase(path::String, name::String, sqlite=true, lake_data::Union{String,Nothing}=nothing, lake_db::Union{String,Nothing}=nothing)
    if sqlite
        return opensqlitedatabase(path, name, lake_data, lake_db)
    else
        error("SQL Server database connections are not yet supported in AHRI_TRE.")
    end
end
"""
    opensqlitedatabase(path::String, name::String, lake_data, lake_db)

Open file on path as an SQLite database (assume .sqlite extension)
    lake_data and lake_db are optional parameters to open a ducklake connection in which to store datasets
    lake_data is the path to the ducklake data directory, and lake_db is the name of the ducklake metadata database file (without extension)
Returns a tuple of the SQLite database connection and the DuckDB connection if lake_data and lake_db are provided.
"""
function opensqlitedatabase(path::String, name::String, lake_data, lake_db)
    @info "Opening SQLite database at $(joinpath(path, "$name.sqlite"))"
    file = joinpath(path, "$name.sqlite")
    if isfile(file)
        db = SQLite.DB(file)
    else
        error("File '$file' not found.")
    end
    conn = nothing
    # Open ducklake database if lake_data and lake_db are provided
    if !isnothing(lake_data) && !isnothing(lake_db)
        @info "Opening DuckDB data lake at $(lake_data) with metadata database $(joinpath(path, "$lake_db.sqlite"))"
        # Ensure the lake_data directory exists
        if !isdir(lake_data)
            mkpath(lake_data)
        end
        metadb = joinpath(path, "$lake_db.sqlite")
        ddb = DuckDB.DB()
        conn = DBInterface.connect(ddb)
        # Attach the data lake database
        DBInterface.execute(conn, "ATTACH 'ducklake:sqlite:$metadb' AS rda_lake (DATA_PATH '$lake_data');")
        DBInterface.execute(conn, "USE rda_lake;")
    end
    return db, conn
end
"""
    opensqlserverdatabase(server::String, name::String)::DBInterface.Connection

Open database 'name' on server 'server' (assume SQL Server database)
"""
function opensqlserverdatabase(server::String, name::String)::DBInterface.Connection
    return ODBC.Connection("Driver=ODBC Driver 17 for SQL Server;Server=$server;Database=$name;Trusted_Connection=yes;")
end
"""
    get_table(db::SQLite.DB, table::String)::AbstractDataFrame

Retrieve table `table` as a DataFrame from `db`
"""
function get_table(db::SQLite.DB, table::String)::AbstractDataFrame
    sql = "SELECT * FROM $(table)"
    df = DBInterface.execute(db, sql) |> DataFrame
    return df
end
"""
    get_table(db::ODBC.Connection, table::String)::AbstractDataFrame

Retrieve table `table` as a DataFrame from `db`
"""
function get_table(db::ODBC.Connection, table::String)::AbstractDataFrame
    sql = "SELECT * FROM $(table)"
    df = DBInterface.execute(db, sql, iterate_rows=true) |> DataFrame
    return df
end
"""
    makeparam(s)

Prepend an @ to the column name to make it a parameter
"""
makeparam(s) = "@" * s

"""
    makeodbcparam(s)

ODBC parameters are ? only instead of @name
"""
makeodbcparam(s) = "?"

"""
    savedataframe(con::DBInterface.Connection, df::AbstractDataFrame, table)

Save a DataFrame to a database table, the names of the dataframe columns should be identical to the table column names in the database
"""
function savedataframe(con::ODBC.Connection, df::AbstractDataFrame, table)
    colnames = names(df)
    paramnames = map(makeodbcparam, colnames) #add @ to column names
    sql = "INSERT INTO $table ($(join(colnames, ", "))) VALUES ($(join(paramnames, ", ")));"
    stmt = DBInterface.prepare(con, sql)
    for row in eachrow(df)
        DBInterface.execute(stmt, Vector(row))
    end
end
"""
    savedataframe(con::SQLite.DB, df::AbstractDataFrame, table)

Save a DataFrame to a database table, the names of the dataframe columns should be identical to the table column names in the database
"""
function savedataframe(con::SQLite.DB, df::AbstractDataFrame, table)
    colnames = names(df)
    paramnames = map(makeparam, colnames) #add @ to column names
    sql = "INSERT INTO $table ($(join(colnames, ", "))) VALUES ($(join(paramnames, ", ")));"
    stmt = DBInterface.prepare(con, sql)
    for row in eachrow(df)
        DBInterface.execute(stmt, NamedTuple(row))
    end
end
"""
    prepareinsertstatement(db::SQLite.DB, table, columns)

Prepare an insert statement for SQLite into table for columns
"""
function prepareinsertstatement(db::SQLite.DB, table, columns)
    paramnames = map(makeparam, columns) # add @ to column name
    sql = "INSERT INTO $table ($(join(columns, ", "))) VALUES ($(join(paramnames, ", ")));"
    return DBInterface.prepare(db, sql)
end
"""
    prepareinsertstatement(db::ODBC.Connection, table, columns)

    Prepare an insert statement for SQL Server into table for columns
"""
function prepareinsertstatement(db::ODBC.Connection, table, columns)
    paramnames = map(makeodbcparam, columns) # ? for each prameter
    sql = "INSERT INTO $table ($(join(columns, ", "))) VALUES ($(join(paramnames, ", ")));"
    return DBInterface.prepare(db, sql)
end

"""
    updatevalue(db::SQLite.DB, table, condition_column, column, condition_value, value)

Update value of column given condition_value in condition_column
"""
function updatevalue(db::SQLite.DB, table, condition_column, column, condition_value, value)
    sql = """
        UPDATE $table 
        SET $column = ?
        WHERE $condition_column = ?
        """
    DBInterface.execute(db, sql, (value, condition_value))
    return nothing
end


"""
    insertwithidentity(db::ODBC.Connection, table, columns, values, keycol)

Insert a record, returning the identity column value
"""
function insertwithidentity(db::ODBC.Connection, table, columns, values, keycol)
    paramnames = map(makeodbcparam, columns) # ? for each prameter
    sql = """
    INSERT INTO $table ($(join(columns, ", "))) 
    OUTPUT INSERTED.$keycol AS last_id
    VALUES ($(join(paramnames, ", ")));
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, values; iterate_rows=true) |> DataFrame
    return df[1, :last_id]
end
"""
    insertwithidentity(db::SQLite.DB, table, columns, values, keycol)

Insert a record, returning the identity column value
"""
function insertwithidentity(db::SQLite.DB, table, columns, values, keycol)
    paramnames = map(makeparam, columns)
    sql = """
    INSERT INTO $table ($(join(columns, ", "))) 
    VALUES ($(join(paramnames, ", ")));
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.lastrowid(DBInterface.execute(stmt, values))
end

"""
    insertdata(db::SQLite.DB, table, columns, values)

Insert a set of values into a table, columns list the names of the columns to insert, and values the values to insert
"""
function insertdata(db::SQLite.DB, table, columns, values)
    stmt = prepareinsertstatement(db, table, columns)
    return DBInterface.execute(stmt, values)
end

"""
    insertdata(db::DBInterface.Connection, table, columns, values, filter)

Insert a set of values into a table, columns list the names of the columns to insert, and values the values to insert
"""
function insertdata(db::DBInterface.Connection, table, columns, values, filter)
    stmt = prepareinsertstatement(db, table, columns, filter)
    return DBInterface.execute(stmt, values)
end

"""
    prepareselectstatement(db::SQLite.DB, table, columns::Vector{String}, filter::Vector{String})

Return a statement to select columns from a table, with 0 to n columns to filter on
"""
function prepareselectstatement(db::SQLite.DB, table, columns::Vector{String}, filter::Vector{String})
    # Start with the SELECT clause
    select_clause = "SELECT " * join(columns, ", ") * " FROM " * table

    # Check if there are any filter conditions and build the WHERE clause
    if isempty(filter)
        return DBInterface.prepare(db, select_clause)
    else
        where_clause = " WHERE " * join(["$col = @$col" for col in filter], " AND ")
        return DBInterface.prepare(db, select_clause * where_clause)
    end
end
"""
    prepareselectstatement(db::ODBC.Connection, table, columns::Vector{String}, filter::Vector{String})

Return a statement to select columns from a table, with 0 to n columns to filter on
"""
function prepareselectstatement(db::ODBC.Connection, table, columns::Vector{String}, filter::Vector{String})
    # Start with the SELECT clause
    select_clause = "SELECT " * join(columns, ", ") * " FROM " * table

    # Check if there are any filter conditions and build the WHERE clause
    if isempty(filter)
        return DBInterface.prepare(db, select_clause)
    else
        where_clause = " WHERE " * join(["$col = ?" for col in filter], " AND ")
        return DBInterface.prepare(db, select_clause * where_clause)
    end
end
"""
    selectdataframe(db::SQLite.DB, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame

Return a dataframe from a table, with 0 to n columns to filter on
"""
function selectdataframe(db::SQLite.DB, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame
    stmt = prepareselectstatement(db, table, columns, filter)
    return DBInterface.execute(stmt, filtervalues) |> DataFrame
end
"""
    selectdataframe(db::ODBC.Connection, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame

Return a dataframe from a table, with 0 to n columns to filter on
"""
function selectdataframe(db::ODBC.Connection, table::String, columns::Vector{String}, filter::Vector{String}, filtervalues::DBInterface.StatementParams)::AbstractDataFrame
    stmt = prepareselectstatement(db, table, columns, filter)
    return DBInterface.execute(stmt, filtervalues; iterate_rows=true) |> DataFrame
end

"""
    selectsourcesites(db::SQLite.DB, source::AbstractSource)

Returns a dataframe with the sites associated with a source
"""
function selectsourcesites(db::SQLite.DB, source::AbstractSource)
    sql = """
    SELECT s.* FROM sites s
    JOIN sources ss ON s.source_id = ss.source_id
    WHERE ss.name = '$(source.name)';
    """
    return DBInterface.execute(db, sql) |> DataFrame
end
"""
    selectsourcesites(db::ODBC.Connection, source::AbstractSource)

Returns a dataframe with the sites associated with a source
"""
function selectsourcesites(db::ODBC.Connection, source::AbstractSource)
    sql = """
    SELECT s.* FROM sites s
    JOIN sources ss ON s.source_id = ss.source_id
    WHERE ss.name = '$(source.name)';
    """
    return DBInterface.execute(db, sql, iterate_rows=true) |> DataFrame
end
"""
    createstudies(db::SQLite.DB)

Creates tables to record a source and associated site/s for deaths contributed to the TRE
"""
function createstudies(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "study_types" (
    "study_type_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE "studies" (
    "study_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "study_type_id" INTEGER,
    CONSTRAINT "fk_sources_study_type_id" FOREIGN KEY ("study_type_id") REFERENCES "study_types" ("study_type_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)

    DBInterface.execute(db, initstudytypes())

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
    createtransformations(db)

Create tables to record data transformations and data ingests
"""
function createtransformations(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "transformation_types" (
    "transformation_type_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_statuses" (
    "transformation_status_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformations" (
    "transformation_id" INTEGER NOT NULL PRIMARY KEY,
    "transformation_type_id" INTEGER NOT NULL,
    "transformation_status_id" INTEGER NOT NULL,
    "description" TEXT NOT NULL,
    "code_reference" BLOB NOT NULL,
    "date_created" DATE NOT NULL,
    "created_by" TEXT NOT NULL,
    CONSTRAINT "fk_transformations_transformation_type_id" FOREIGN KEY ("transformation_type_id") REFERENCES "transformation_types" ("transformation_type_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_transformations_transformation_status_id" FOREIGN KEY ("transformation_status_id") REFERENCES "transformation_statuses" ("transformation_status_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "data_ingestions" (
    "data_ingestion_id" INTEGER NOT NULL PRIMARY KEY,
    "study_id" INTEGER NOT NULL,
    "date_received" DATE NOT NULL,
    "description" TEXT,
    CONSTRAINT "fk_data_ingestions_source_id" FOREIGN KEY ("study_id") REFERENCES "studies" ("study_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    types = inittypes()
    statuses = initstatuses()
    savedataframe(db, types, "transformation_types")
    savedataframe(db, statuses, "transformation_statuses")
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
    createvariables(db)

Create tables to record value types, variables and vocabularies
"""
function createvariables(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "value_types" (
    "value_type_id" INTEGER NOT NULL PRIMARY KEY,
    "value_type" TEXT NOT NULL,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_value_type"
    ON "value_types" (
    "value_type" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "vocabularies" (
    "vocabulary_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "description" TEXT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "vocabulary_items" (
    "vocabulary_item_id" INTEGER NOT NULL PRIMARY KEY,
    "vocabulary_id" INTEGER NOT NULL,
    "value" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "description" TEXT,
    CONSTRAINT "fk_vocabulary_items" FOREIGN KEY ("vocabulary_id") REFERENCES "vocabularies"("vocabulary_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "domains" (
    "domain_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_domain_name"
    ON "domains" (
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "variables" (
    "variable_id" INTEGER NOT NULL PRIMARY KEY,
    "domain_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "value_type_id" INTEGER NOT NULL,
    "vocabulary_id" INTEGER,
    "description" TEXT,
    "note" TEXT,
    "keyrole" TEXT,
    CONSTRAINT "fk_variables_domain_id" FOREIGN KEY ("domain_id") REFERENCES "domains"("domain_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_variables_value_type_id" FOREIGN KEY ("value_type_id") REFERENCES "value_types"("value_type_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_variables_vocabulary_id" FOREIGN KEY ("vocabulary_id") REFERENCES "vocabularies"("vocabulary_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_variables_domain_name"
    ON "variables" (
    "domain_id" ASC,
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "vocabulary_mapping" (
    "vocabulary_mapping_id" INTEGER NOT NULL PRIMARY KEY,
    "from_vocabulary_item" INTEGER NOT NULL,
    "to_vocabulary_item" INTEGER NOT NULL,
    CONSTRAINT "fk_vocabulary_mapping" FOREIGN KEY ("from_vocabulary_item") REFERENCES "vocabulary_items" ("vocabulary_item_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_vocabulary_mapping" FOREIGN KEY ("to_vocabulary_item") REFERENCES "vocabulary_items" ("vocabulary_item_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    types = initvalue_types()
    SQLite.load!(types, db, "value_types")
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
function identityinserton(db::ODBC.Connection, table::String)
    sql = "SET IDENTITY_INSERT [$table] ON"
    DBInterface.execute(db, sql)
    return nothing
end
function identityinsertoff(db::ODBC.Connection, table::String)
    sql = "SET IDENTITY_INSERT [$table] OFF"
    DBInterface.execute(db, sql)
    return nothing
end

"""
    updatevariable_vocabulary(db::DBInterface.Connection, name, domain_id, vocabulary_id)

Update variable vocabulary
"""
function updatevariable_vocabulary(db::DBInterface.Connection, name, domain_id, vocabulary_id)
    sql = """
    UPDATE variables
      SET vocabulary_id = $vocabulary_id
    WHERE name LIKE '%$name%'
      AND domain_id = $domain_id
    """
    DBInterface.execute(db, sql)
end
"""
    createdatasets(db::SQLite.DB)

Create tables to record datasets, rows, data and links to the transformations that use/created the datasets
"""
function createdatasets(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "datasets" (
    "dataset_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "date_created" DATE NOT NULL,
    "description" TEXT,
    "unit_of_analysis_id" INTEGER,
    "repository_id" TEXT,
    "doi" TEXT,
    "in_lake" TINYINT NOT NULL DEFAULT 0, -- 0 = false, 1 = true if dataset is in the TRE lake
    CONSTRAINT "fk_datasets_unit_of_analysis_id" FOREIGN KEY ("unit_of_analysis_id") REFERENCES "unit_of_analysis_types" ("unit_of_analysis_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_datasets_repository_id" FOREIGN KEY ("repository_id") REFERENCES "repository" ("repository_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE "repository" (
    "repository_id" TEXT NOT NULL PRIMARY KEY,
    "repository_ddi_id" TEXT,
    "repository_ddi" BLOB,
    "repository_rdf" BLOB,
    CONSTRAINT "fk_repository_dataset_id" FOREIGN KEY ("repository_id") REFERENCES "datasets" ("repository_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE "unit_of_analysis_types" (
    "unit_of_analysis_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    units = initunitanalysis()
    savedataframe(db, units, "unit_of_analysis_types")

    sql = raw"""
    CREATE TABLE "datarows" (
    "row_id" INTEGER NOT NULL PRIMARY KEY,
    "dataset_id" INTEGER NOT NULL,
    CONSTRAINT "fk_datarows_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_inputs" (
    "transformation_id" INTEGER NOT NULL,
    "dataset_id" INTEGER NOT NULL,
    PRIMARY KEY ("transformation_id", "dataset_id"),
    CONSTRAINT "fk_transformation_inputs_transformation_id" FOREIGN KEY ("transformation_id") REFERENCES "transformations" ("transformation_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_transformation_inputs_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "transformation_outputs" (
    "transformation_id" INTEGER NOT NULL,
    "dataset_id" INTEGER NOT NULL,
    PRIMARY KEY ("transformation_id", "dataset_id"),
    CONSTRAINT "fk_transformation_outputs_transformation_id" FOREIGN KEY ("transformation_id") REFERENCES "transformations" ("transformation_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_transformation_outputs_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "dataset_variables" (
    "dataset_id" INTEGER NOT NULL,
    "variable_id" INTEGER NOT NULL,
    PRIMARY KEY ("dataset_id", "variable_id"),
    CONSTRAINT "fk_dataset_variables_variable_id" FOREIGN KEY ("variable_id") REFERENCES "variables" ("variable_id") ON DELETE NO ACTION ON UPDATE RESTRICT,
    CONSTRAINT "fk_dataset_variables_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "ingest_datasets" (
        ingest_dataset_id INTEGER NOT NULL PRIMARY KEY,
        data_ingestion_id INTEGER NOT NULL,
        transformation_id INTEGER NOT NULL,
        dataset_id INTEGER NOT NULL,
        CONSTRAINT "fk_ingest_datasets_data_ingestion_id" FOREIGN KEY ("data_ingestion_id") REFERENCES "data_ingestions" ("data_ingestion_id") ON DELETE CASCADE ON UPDATE RESTRICT,
        CONSTRAINT "fk_ingest_datasets_transformation_id" FOREIGN KEY ("transformation_id") REFERENCES "transformations" ("transformation_id") ON DELETE CASCADE ON UPDATE RESTRICT,
        CONSTRAINT "fk_ingest_datasets_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE NO ACTION ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end

"""
    initunitanalysis()

Default unit of analysis
"""
initunitanalysis() = DataFrame([(unit_of_analysis_id=TRE_UNIT_OF_ANALYSIS_INDIVIDUAL, name="Individual"),
    (unit_of_analysis_id=TRE_UNIT_OF_ANALYSIS_AGGREGATION, name="Aggregation")])
"""
    createentities(db)

Create tables to store deaths, and their association with data rows and data ingests
"""
function createentities(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "entities" (
    "entity_id" INTEGER NOT NULL PRIMARY KEY,
    "study_id" INTEGER NOT NULL,
    "external_id" TEXT NOT NULL,
    "data_ingestion_id" INTEGER NOT NULL,
    CONSTRAINT "fk_deaths_study_id" FOREIGN KEY ("study_id") REFERENCES "studies" ("study_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_deaths_data_ingestion_id" FOREIGN KEY ("data_ingestion_id") REFERENCES "data_ingestions" ("data_ingestion_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "unique_external_id" UNIQUE ("study_id" ASC, "external_id" ASC)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE INDEX "i_deaths_study_id"
    ON "deaths" (
    "study_id" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "entity_rows" (
    "entity_id" INTEGER NOT NULL,
    "row_id" INTEGER NOT NULL,
    PRIMARY KEY ("entity_id", "row_id"),
    CONSTRAINT "fk_entity_rows_entity_id" FOREIGN KEY ("entity_id") REFERENCES "entities" ("entity_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_entity_rows_row_id" FOREIGN KEY ("row_id") REFERENCES "datarows" ("row_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "unique_rows" UNIQUE ("entity_id" ASC, "row_id" ASC)
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end

"""
    createmapping(db::SQLite.DB)

Create the table required for variable mapping. This table is used to map variables from one instrument to another. The table is created in the database provided as an argument.
The variable mapping is based on the PyCrossVA approach.

The relationship to the PyCrossVA configuration file columns:

  * New Column Name = destination_id - the variable_id of the new column
  * New Column Documentation = Stored in the variable table
  * Source Column ID = from_id - the variable_id of the source variable
  * Source Column Documentation = will be in the variables table
  * Relationship = operator - the operator to be used to create the new variable
  * Condition = operants - the operants to be used with the operator
  * Prerequisite = prerequisite_id - the variable_id of the prerequisite variable

"""
function createmapping(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "variable_mapping" (
    "mapping_id" INTEGER NOT NULL PRIMARY KEY,
    "from_variable_id" INTEGER NOT NULL,
    "to_variable_id" INTEGER NOT NULL,
    "operator" TEXT NOT NULL,
    "operants" TEXT NOT NULL,
    "prerequisite_id" INTEGER,
    CONSTRAINT "fk_variable_mapping_from_variable_id" FOREIGN KEY ("from_variable_id") REFERENCES "variables" ("variable_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_variable_mapping_to_variable_id" FOREIGN KEY ("to_variable_id") REFERENCES "variables" ("variable_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_variable_mapping_prerequisite_id" FOREIGN KEY ("prerequisite_id") REFERENCES "variables" ("variable_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end