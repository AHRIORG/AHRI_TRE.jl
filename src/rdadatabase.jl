"""
    createdatabase(path, name; replace=false, sqlite=true)

Creates a database to store the information contained in the Reference Death Archive (RDA)
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
        createsources(db)
        createprotocols(db)
        createtransformations(db)
        createvariables(db)
        createdatasets(db)
        createinstruments(db)
        createdeaths(db)
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
    opendatabase(path::String, name::String; sqlite = true)::DBInterface.Connection

If sqlite = true (default) open file on path as an SQLite database (assume .sqlite extension)
else open database 'name' on server 'path' (assume SQL Server database)
"""
function opendatabase(path::String, name::String; sqlite=true)::DBInterface.Connection
    if sqlite
        return opensqlitedatabase(path, name)
    else
        return opensqlserverdatabase(path, name)
    end
end
"""
    opensqlitedatabase(path::String, name::String)::DBInterface.Connection

Open file on path as an SQLite database (assume .sqlite extension)
"""
function opensqlitedatabase(path::String, name::String)::DBInterface.Connection
    file = joinpath(path, "$name.sqlite")
    if isfile(file)
        return SQLite.DB(file)
    else
        error("File '$file' not found.")
    end
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
    createsources(db::SQLite.DB)

Creates tables to record a source and associated site/s for deaths contributed to the RDA
"""
function createsources(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "sources" (
    "source_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "study_type_id" INTEGER,
    CONSTRAINT "fk_sources_study_type_id" FOREIGN KEY ("study_type_id") REFERENCES "study_types" ("study_type_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE "study_types" (
    "study_type_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE "sites" (
    "site_id" INTEGER NOT NULL PRIMARY KEY,
    "site_name" TEXT NOT NULL,
    "country_name" TEXT NOT NULL,
    "country_iso3" TEXT NOT NULL,
    "source_id" INTEGER NOT NULL,
    CONSTRAINT "fk_sites_source_id" FOREIGN KEY ("source_id") REFERENCES "sources" ("source_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_source_name"
    ON "sites" (
    "source_id" ASC,
    "site_name" ASC
    );
    """
    DBInterface.execute(db, sql)

    study_types = initstudytypes()
    savedataframe(db, study_types, "study_types")
    
    return nothing
end

"""
    initstudytypes()

Default transformation types
"""
initstudytypes() = DataFrame([(study_type_id=RDA_STUDY_TYPE_DSS, name="Demographic Surveillance"),
    (study_type_id=RDA_STUDY_TYPE_COHORT, name="Cohort study"),
    (study_type_id=RDA_STUDY_TYPE_SURVEY, name="Cross-sectional survey"),
    (study_type_id=RDA_STUDY_TYPE_PANEL, name="Panel data")])

"""
    createsources(db::ODBC.Connection)

Creates tables to record a source and associated site/s for deaths contributed to the RDA
"""
function createsources(db::ODBC.Connection)
    sql = raw"""
    CREATE TABLE [sources] (
        [source_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL,
        [pi_name] NVARCHAR(255) NOT NULL,
        [pi_affiliation] NVARCHAR(255) NOT NULL,
        [funder] NVARCHAR(255) NOT NULL,
        [funder_abbr] NVARCHAR(255) NOT NULL
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE [sites] (
        [site_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [site_name] NVARCHAR(255) NOT NULL,
        [country_name] NVARCHAR(255) NOT NULL,
        [country_iso3] NCHAR(3) NOT NULL,
        [source_id] INT NOT NULL,
        CONSTRAINT [fk_sites_source_id] FOREIGN KEY ([source_id]) REFERENCES [sources] ([source_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX [i_source_name]
    ON [sites] (
        [source_id] ASC,
        [site_name] ASC
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE [source_collection] (
        [source_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [source_cycle] NVARCHAR(255),
        [collection_start] NVARCHAR(255),
        [collection_end] NVARCHAR(255),
        [period_start] NVARCHAR(255),
        [period_end] NVARCHAR(255),
        PRIMARY KEY ([source_id]),
        CONSTRAINT [fk_sites_source_id] FOREIGN KEY ([source_id]) REFERENCES [sources] ([source_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)

    return nothing
end

"""
    createprotocols(db::SQLite.DB)

Create tables to record information about protocols and the ethics approvals for those protocols
"""
function createprotocols(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "ethics" (
    "ethics_id" INTEGER NOT NULL PRIMARY KEY,
    "source_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "ethics_committee" TEXT NOT NULL,
    "ethics_reference" TEXT NOT NULL,
    CONSTRAINT "fk_ethics_source_id" FOREIGN KEY ("source_id") REFERENCES "sources" ("source_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "ethics_documents" (
    "ethics_document_id" INTEGER NOT NULL PRIMARY KEY,
    "ethics_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT NULL,
    "document" BLOB,
    CONSTRAINT "fk_ethics_documents_ethics_id" FOREIGN KEY ("ethics_id") REFERENCES "ethics" ("ethics_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_ethic_name"
    ON "ethics_documents" (
    "ethics_id" ASC,
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "protocols" (
    "protocol_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL,
    "ethics_id" INTEGER,
    CONSTRAINT "fk_protocols_ethics_id" FOREIGN KEY ("ethics_id") REFERENCES "ethics" ("ethics_id") ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_protocol_name"
    ON "protocols" (
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "site_protocols" (
    "site_id" INTEGER NOT NULL,
    "protocol_id" INTEGER NOT NULL,
    PRIMARY KEY ("site_id", "protocol_id"),
    CONSTRAINT "fk_site_protocols_site_id" FOREIGN KEY ("site_id") REFERENCES "sites" ("site_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_site_protocols_protocol_id" FOREIGN KEY ("protocol_id") REFERENCES "protocols" ("protocol_id") ON DELETE CASCADE ON UPDATE RESTRICT
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "protocol_documents" (
    "protocol_document_id" INTEGER NOT NULL PRIMARY KEY,
    "protocol_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "document" BLOB,
    CONSTRAINT "fk_protocol_documents_protocol_id" FOREIGN KEY ("protocol_id") REFERENCES "protocols" ("protocol_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createprotocols(db::ODBC.Connection)

Create tables to record information about protocols and the ethics approvals for those protocols
"""
function createprotocols(db::ODBC.Connection)
    sql = raw"""
    CREATE TABLE [ethics] (
        [ethics_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [source_id] INT NOT NULL,
        [name] NVARCHAR(255) NOT NULL,
        [ethics_committee] NVARCHAR(255) NOT NULL,
        [ethics_reference] NVARCHAR(255) NOT NULL,
        CONSTRAINT [fk_ethics_source_id] FOREIGN KEY ([source_id]) REFERENCES [sources] ([source_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [ethics_documents] (
        [ethics_document_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [ethics_id] INT NOT NULL,
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX) NULL,
        [document] VARBINARY(MAX),  -- This is the closest equivalent to BLOB in SQL Server
        CONSTRAINT [fk_ethics_documents_ethics_id] FOREIGN KEY ([ethics_id]) REFERENCES [ethics] ([ethics_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX [i_ethic_name]
    ON [ethics_documents] (
        [ethics_id] ASC,
        [name] ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [protocols] (
        [protocol_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX) NOT NULL,
        [ethics_id] INT,
        CONSTRAINT [fk_protocols_ethics_id] FOREIGN KEY ([ethics_id]) REFERENCES [ethics] ([ethics_id]) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX [i_protocol_name]
    ON [protocols] (
        [name] ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [site_protocols] (
        [site_id] INT NOT NULL,
        [protocol_id] INT NOT NULL,
        PRIMARY KEY ([site_id], [protocol_id]),
        CONSTRAINT [fk_site_protocols_site_id] FOREIGN KEY ([site_id]) REFERENCES [sites] ([site_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_site_protocols_protocol_id] FOREIGN KEY ([protocol_id]) REFERENCES [protocols] ([protocol_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [protocol_documents] (
        [protocol_document_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [protocol_id] INT NOT NULL,
        [name] NVARCHAR(255) NOT NULL,
        [document] VARBINARY(MAX),  -- BLOB equivalent in SQL Server
        CONSTRAINT [fk_protocol_documents_protocol_id] FOREIGN KEY ([protocol_id]) REFERENCES [protocols] ([protocol_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
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
    "source_id" INTEGER NOT NULL,
    "date_received" DATE NOT NULL,
    "description" TEXT,
    CONSTRAINT "fk_data_ingestions_source_id" FOREIGN KEY ("source_id") REFERENCES "sources" ("source_id") ON DELETE CASCADE ON UPDATE RESTRICT
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
inittypes() = DataFrame([(transformation_type_id=RDA_TRANSFORMATION_TYPE_INGEST, name="Raw data ingest"),
    (transformation_type_id=RDA_TRANSFORMATION_TYPE_TRANSFORM, name="Dataset transform")])
"""
    initstatuses()

Default transformation statuses
"""
initstatuses() = DataFrame([(transformation_status_id=RDA_TRANSFORMATION_STATUS_UNVERIFIED, name="Unverified"),
    (transformation_status_id=RDA_TRANSFORMATION_STATUS_VERIFIED, name="Verified")])

"""
    createtransformations(db::ODBC.Connection)

Create tables to record data transformations and data ingests
"""
function createtransformations(db::ODBC.Connection)
    sql = raw"""
    CREATE TABLE [transformation_types] (
        [transformation_type_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [transformation_statuses] (
        [transformation_status_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [transformations] (
        [transformation_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [transformation_type_id] INT NOT NULL,
        [transformation_status_id] INT NOT NULL,
        [description] NVARCHAR(MAX) NOT NULL,
        [code_reference] VARBINARY(MAX) NOT NULL,
        [date_created] DATE NOT NULL,
        [created_by] NVARCHAR(255) NOT NULL,
        CONSTRAINT [fk_transformations_transformation_type_id] FOREIGN KEY ([transformation_type_id]) REFERENCES [transformation_types] ([transformation_type_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_transformations_transformation_status_id] FOREIGN KEY ([transformation_status_id]) REFERENCES [transformation_statuses] ([transformation_status_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [data_ingestions] (
        [data_ingestion_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [source_id] INT NOT NULL,
        [date_received] DATE NOT NULL,
        [description] NVARCHAR(MAX),
        CONSTRAINT [fk_data_ingestions_source_id] FOREIGN KEY ([source_id]) REFERENCES [sources] ([source_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    types = inittypes()
    statuses = initstatuses()
    identityinserton(db, "transformation_types")
    savedataframe(db, types, "transformation_types")
    identityinsertoff(db, "transformation_types")
    identityinserton(db, "transformation_statuses")
    savedataframe(db, statuses, "transformation_statuses")
    identityinsertoff(db, "transformation_statuses")
    return nothing
end
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
    createvariables(db::ODBC.Connection)

Create tables to record value types, variables and vocabularies
"""
function createvariables(db::ODBC.Connection)
    sql = raw"""
    CREATE TABLE [value_types] (
        [value_type_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [value_type] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX [i_value_type]
    ON [value_types] (
        [value_type] ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [vocabularies] (
        [vocabulary_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [vocabulary_items] (
        [vocabulary_item_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [vocabulary_id] INT NOT NULL,
        [value] NVARCHAR(255) NOT NULL,
        [code] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX),
        CONSTRAINT [fk_vocabulary_items] FOREIGN KEY ([vocabulary_id]) 
            REFERENCES [vocabularies]([vocabulary_id]) 
            ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [domains] (
        [domain_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX) NOT NULL
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX [i_domain_name]
    ON [domains] (
        [name] ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [variables] (
        [variable_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [domain_id] INT NOT NULL,
        [name] NVARCHAR(255) NOT NULL,
        [value_type_id] INT NOT NULL,
        [vocabulary_id] INT,
        [description] NVARCHAR(MAX),
        [note] NVARCHAR(MAX),
        [keyrole] NVARCHAR(255),
        CONSTRAINT [fk_variables_domain_id] FOREIGN KEY ([domain_id]) 
            REFERENCES [domains]([domain_id]) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT [fk_variables_value_type_id] FOREIGN KEY ([value_type_id]) 
            REFERENCES [value_types]([value_type_id]) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT [fk_variables_vocabulary_id] FOREIGN KEY ([vocabulary_id]) 
            REFERENCES [vocabularies]([vocabulary_id]) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX [i_variables_domain_name]
    ON [variables] (
        [domain_id] ASC,
        [name] ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [vocabulary_mapping] (
        [vocabulary_mapping_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [from_vocabulary_item] INT NOT NULL,
        [to_vocabulary_item] INT NOT NULL,
        CONSTRAINT [fk_vocabulary_mapping_from] FOREIGN KEY ([from_vocabulary_item]) REFERENCES [vocabulary_items] ([vocabulary_item_id]) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT [fk_vocabulary_mapping_to] FOREIGN KEY ([to_vocabulary_item]) REFERENCES [vocabulary_items] ([vocabulary_item_id]) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    types = initvalue_types()
    identityinserton(db, "value_types")
    savedataframe(db, types, "value_types")
    identityinsertoff(db, "value_types")
    return nothing
end
"""
    initvalue_types()

Add default value types
"""
initvalue_types() = DataFrame([(value_type_id=RDA_TYPE_INTEGER, value_type="Integer", description=""),
    (value_type_id=RDA_TYPE_FLOAT, value_type="Float", description=""),
    (value_type_id=RDA_TYPE_STRING, value_type="String", description=""),
    (value_type_id=RDA_TYPE_DATE, value_type="Date", description="ISO Date yyyy-mm-dd"),
    (value_type_id=RDA_TYPE_DATETIME, value_type="Datetime", description="ISO Datetime yyyy-mm-ddTHH:mm:ss.sss"),
    (value_type_id=RDA_TYPE_TIME, value_type="Time", description="ISO Time HH:mm:ss.sss"),
    (value_type_id=RDA_TYPE_CATEGORY, value_type="Categorical", description="Category represented by a Vocabulary with integer value and string code, stored as Integer")
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
    CREATE TABLE "data" (
    "row_id" INTEGER NOT NULL,
    "variable_id" INTEGER NOT NULL,
    "value" TEXT NULL,
    PRIMARY KEY ("row_id", "variable_id"),
    CONSTRAINT "fk_data_row_id" FOREIGN KEY ("row_id") REFERENCES "datarows" ("row_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "fk_data_variable_id" FOREIGN KEY ("variable_id") REFERENCES "variables" ("variable_id") ON DELETE CASCADE ON UPDATE RESTRICT,
    CONSTRAINT "unique_data" UNIQUE ("row_id" ASC, "variable_id" ASC)
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
initunitanalysis() = DataFrame([(unit_of_analysis_id=RDA_UNIT_OF_ANALYSIS_INDIVIDUAL, name="Individual"),
    (unit_of_analysis_id=RDA_UNIT_OF_ANALYSIS_AGGREGATION, name="Aggregation")])

"""
    createdatasets(db::ODBC.Connection)

Create tables to record datasets, rows, data and links to the transformations that use/created the datasets
"""
function createdatasets(db::ODBC.Connection)
    sql = raw"""
    CREATE TABLE [datasets] (
        [dataset_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL,
        [date_created] DATE NOT NULL,
        [description] NVARCHAR(MAX),

        [unit_of_analysis_id] INTEGER,
        [repository_id] NVARCHAR(255),
        [doi] NVARCHAR(255)
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE [repository] (
        [repository_id] NVARCHAR(255) NOT NULL,
        [repository_ddi_id] NVARCHAR(255),
        [repository_ddi] VARBINARY(MAX),
        [repository_rdf] VARBINARY(MAX),
        PRIMARY KEY ([repository_id]),
        CONSTRAINT [fk_repository_id] FOREIGN KEY ([repository_id]) REFERENCES [datasets] ([repository_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE [datarows] (
        [row_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [dataset_id] INT NOT NULL,
        CONSTRAINT [fk_datarows_dataset_id] FOREIGN KEY ([dataset_id]) REFERENCES [datasets] ([dataset_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    
    sql = raw"""
    CREATE TABLE [data] (
        [row_id] INT NOT NULL,
        [variable_id] INT NOT NULL,
        [value_integer] INT NULL,
        [value_float] FLOAT NULL,
        [value_string] NVARCHAR(4000) NULL,
        [value_datetime] DATETIME NULL,
        [value_binary] VARBINARY(MAX) NULL,
        PRIMARY KEY ([row_id], [variable_id]),
        CONSTRAINT [fk_data_row_id] FOREIGN KEY ([row_id]) REFERENCES [datarows] ([row_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_data_variable_id] FOREIGN KEY ([variable_id]) REFERENCES [variables] ([variable_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [transformation_inputs] (
        [transformation_id] INT NOT NULL,
        [dataset_id] INT NOT NULL,
        PRIMARY KEY ([transformation_id], [dataset_id]),
        CONSTRAINT [fk_transformation_inputs_transformation_id] FOREIGN KEY ([transformation_id]) REFERENCES [transformations] ([transformation_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_transformation_inputs_dataset_id] FOREIGN KEY ([dataset_id]) REFERENCES [datasets] ([dataset_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [transformation_outputs] (
        [transformation_id] INT NOT NULL,
        [dataset_id] INT NOT NULL,
        PRIMARY KEY ([transformation_id], [dataset_id]),
        CONSTRAINT [fk_transformation_outputs_transformation_id] FOREIGN KEY ([transformation_id]) REFERENCES [transformations] ([transformation_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_transformation_outputs_dataset_id] FOREIGN KEY ([dataset_id]) REFERENCES [datasets] ([dataset_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [dataset_variables] (
        [dataset_id] INT NOT NULL,
        [variable_id] INT NOT NULL,
        PRIMARY KEY ([dataset_id], [variable_id]),
        CONSTRAINT [fk_dataset_variables_variable_id] FOREIGN KEY ([variable_id]) REFERENCES [variables] ([variable_id]) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT [fk_dataset_variables_dataset_id] FOREIGN KEY ([dataset_id]) REFERENCES [datasets] ([dataset_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [ingest_datasets] (
        [ingest_dataset_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [data_ingestion_id] INT NOT NULL,
        [transformation_id] INT NOT NULL,
        [dataset_id] INT NOT NULL,
        CONSTRAINT [fk_ingest_datasets_data_ingestion_id] FOREIGN KEY ([data_ingestion_id]) REFERENCES [data_ingestions] ([data_ingestion_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_ingest_datasets_transformation_id] FOREIGN KEY ([transformation_id]) REFERENCES [transformations] ([transformation_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_ingest_datasets_dataset_id] FOREIGN KEY ([dataset_id]) REFERENCES [datasets] ([dataset_id]) ON DELETE NO ACTION ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createinstruments(db::SQLite.DB)

Create tables to record data collection instruments, and their associated protocols and datasets
"""
function createinstruments(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "instruments" (
    "instrument_id" INTEGER NOT NULL PRIMARY KEY,
    "name" TEXT NOT NULL,
    "description" TEXT NOT NULL,
    "collection_cycle" TEXT,
    "collection_start" DATE,
    "collection_end" DATE
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX "i_instrument_name"
    ON "instruments" (
    "name" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "instrument_datasets" (
    "instrument_id" INTEGER NOT NULL,
    "dataset_id" INTEGER NOT NULL,
    PRIMARY KEY ("instrument_id", "dataset_id"),
    CONSTRAINT "fk_instrument_datasets_instrument_id" FOREIGN KEY ("instrument_id") REFERENCES "instruments" ("instrument_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_instrument_datasets_dataset_id" FOREIGN KEY ("dataset_id") REFERENCES "datasets" ("dataset_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)

    sql = raw"""
    CREATE TABLE "instrument_documents" (
    "intrument_document_id" INTEGER NOT NULL,
    "instrument_id" INTEGER NOT NULL,
    "name" TEXT NOT NULL,
    "document" BLOB,
    PRIMARY KEY ("intrument_document_id"),
    CONSTRAINT "fk_instrument_documents_instrument_id" FOREIGN KEY ("instrument_id") REFERENCES "instruments" ("instrument_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "u_instrument_documents" UNIQUE ("instrument_id" ASC, "name" ASC)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "protocol_instruments" (
    "protocol_id" INTEGER NOT NULL,
    "instrument_id" INTEGER NOT NULL,
    PRIMARY KEY ("protocol_id", "instrument_id"),
    CONSTRAINT "fk_protocol_instruments_protocol_id" FOREIGN KEY ("protocol_id") REFERENCES "protocols" ("protocol_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_protocol_instruments" FOREIGN KEY ("instrument_id") REFERENCES "instruments" ("instrument_id") ON DELETE CASCADE ON UPDATE NO ACTION
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createinstruments(db::ODBC.Connection)

Create tables to record data collection instruments, and their associated protocols and datasets
"""
function createinstruments(db::ODBC.Connection)
    sql = raw"""
    CREATE TABLE [instruments] (
        [instrument_id] INT NOT NULL PRIMARY KEY IDENTITY(1,1),
        [name] NVARCHAR(255) NOT NULL,
        [description] NVARCHAR(MAX) NOT NULL,
        [collection_cycle] NVARCHAR(255),
        [collection_start] DATE,
        [collection_end] DATE
    )
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE UNIQUE INDEX [i_instrument_name]
    ON [instruments] (
        [name] ASC
    )
    """
    DBInterface.execute(db, sql)
    # !!! NEEDS UPDATE TO instrument_datasets
    sql = raw"""
    CREATE TABLE [instrument_datasets] (
        [instrument_id] INTEGER NOT NULL,
        [dataset_id] INTEGER NOT NULL,
        PRIMARY KEY ([instrument_id], [dataset_id]),
        CONSTRAINT [fk_instrument_datasets_instrument_id] FOREIGN KEY ([instrument_id]) REFERENCES [instruments] ([instrument_id]) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT [fk_instrument_datasets_dataset_id] FOREIGN KEY ([dataset_id]) REFERENCES [datasets] ([dataset_id]) ON DELETE CASCADE ON UPDATE NO ACTION
    )
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [instrument_documents] (
        [intrument_document_id] INTEGER NOT NULL PRIMARY KEY IDENTITY(1,1),
        [instrument_id] INTEGER NOT NULL,
        [name] NVARCHAR(255) NOT NULL,
        [document] VARBINARY(MAX),
        CONSTRAINT [fk_instrument_documents_instrument_id] FOREIGN KEY ([instrument_id]) REFERENCES [instruments] ([instrument_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [u_instrument_documents] UNIQUE ([instrument_id], [name])
    )
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [protocol_instruments] (
        [protocol_id] INT NOT NULL,
        [instrument_id] INT NOT NULL,
        PRIMARY KEY ([protocol_id], [instrument_id]),
        CONSTRAINT [fk_protocol_instruments_protocol_id] FOREIGN KEY ([protocol_id]) REFERENCES [protocols] ([protocol_id]) ON DELETE CASCADE,
        CONSTRAINT [fk_protocol_instruments_instrument_id] FOREIGN KEY ([instrument_id]) REFERENCES [instruments] ([instrument_id]) ON DELETE CASCADE
    )
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createdeaths(db)

Create tables to store deaths, and their association with data rows and data ingests
"""
function createdeaths(db::SQLite.DB)
    sql = raw"""
    CREATE TABLE "deaths" (
    "death_id" INTEGER NOT NULL PRIMARY KEY,
    "site_id" INTEGER NOT NULL,
    "external_id" TEXT NOT NULL,
    "data_ingestion_id" INTEGER NOT NULL,
    CONSTRAINT "fk_deaths_site_id" FOREIGN KEY ("site_id") REFERENCES "sites" ("site_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "fk_deaths_data_ingestion_id" FOREIGN KEY ("data_ingestion_id") REFERENCES "data_ingestions" ("data_ingestion_id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "unique_external_id" UNIQUE ("site_id" ASC, "external_id" ASC)
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE INDEX "i_deaths_site_id"
    ON "deaths" (
    "site_id" ASC
    );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE "death_rows" (
    "death_id" INTEGER NOT NULL,
    "row_id" INTEGER NOT NULL,
    PRIMARY KEY ("death_id", "row_id"),
    CONSTRAINT "fk_death_rows_death_id" FOREIGN KEY ("death_id") REFERENCES "deaths" ("death_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "fk_death_rows_row_id" FOREIGN KEY ("row_id") REFERENCES "datarows" ("row_id") ON DELETE CASCADE ON UPDATE NO ACTION,
    CONSTRAINT "unique_rows" UNIQUE ("death_id" ASC, "row_id" ASC)
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end
"""
    createdeaths(db::ODBC.Connection)

Create tables to store deaths, and their association with data rows and data ingests
"""
function createdeaths(db::ODBC.Connection)
    sql = raw"""
    CREATE TABLE [deaths] (
        [death_id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [site_id] INT NOT NULL,
        [external_id] NVARCHAR(127) NOT NULL,
        [data_ingestion_id] INT NOT NULL,
        CONSTRAINT [fk_deaths_site_id] FOREIGN KEY ([site_id]) REFERENCES [sites] ([site_id]) ON DELETE NO ACTION,
        CONSTRAINT [fk_deaths_data_ingestion_id] FOREIGN KEY ([data_ingestion_id]) REFERENCES [data_ingestions] ([data_ingestion_id]) ON DELETE NO ACTION,
        CONSTRAINT [unique_external_id] UNIQUE ([site_id], [external_id])
    )
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE INDEX [i_deaths_site_id]
    ON [deaths] ([site_id]);
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE [death_rows] (
        [death_id] INT NOT NULL,
        [row_id] INT NOT NULL,
        PRIMARY KEY ([death_id], [row_id]),
        CONSTRAINT [fk_death_rows_death_id] FOREIGN KEY ([death_id]) REFERENCES [deaths] ([death_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [fk_death_rows_row_id] FOREIGN KEY ([row_id]) REFERENCES [datarows] ([row_id]) ON DELETE CASCADE ON UPDATE NO ACTION,
        CONSTRAINT [unique_rows] UNIQUE ([death_id], [row_id])
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
        CREATE TABLE IF NOT EXISTS "variablemaps" (
            "variablemap_id"	INTEGER NOT NULL PRIMARY KEY,
            "name"	TEXT NOT NULL,
            "instrument_id"	INTEGER NULL,
            "source_domain" TEXT NULL,
            "destination_domain" TEXT NULL,
            CONSTRAINT "fk_variablemaps_instruments" FOREIGN KEY("instrument_id") REFERENCES "instruments"("instrument_id")
        );
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    CREATE TABLE IF NOT EXISTS variablemappings (
        variablemapping_id INTEGER NOT NULL PRIMARY KEY,
        variablemap_id INTEGER NOT NULL,
        destination_id INTEGER NOT NULL,
        from_id INTEGER NULL,
        operator TEXT NULL,
        operants TEXT NULL,
        prerequisite_id INTEGER NULL,
        CONSTRAINT fk_variablemappings_variablemap_id FOREIGN KEY (variablemap_id) REFERENCES variablemaps (variablemap_id),
        CONSTRAINT fk_variablemappings_destination_id FOREIGN KEY (destination_id) REFERENCES variables (variable_id),
        CONSTRAINT fk_variablemappings_from_id FOREIGN KEY (from_id) REFERENCES variables (variable_id),
        CONSTRAINT fk_variablemappings_prerequisite_id FOREIGN KEY (prerequisite_id) REFERENCES variables (variable_id)
    );
    """
    DBInterface.execute(db, sql)
    return nothing
end

"""
    createmapping(db::ODBC.Connection)

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
function createmapping(db::ODBC.Connection)
    sql = raw"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'variablemaps')
    BEGIN
        CREATE TABLE [variablemaps] (
            [variablemap_id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [name] NVARCHAR(255) NOT NULL,
            [instrument_id] INT NULL,
            [source_domain] NVARCHAR(255) NULL,
            [destination_domain] NVARCHAR(255) NULL,
            CONSTRAINT [fk_variablemaps_instruments] FOREIGN KEY ([instrument_id]) REFERENCES [instruments]([instrument_id])
        );
    END
    """
    DBInterface.execute(db, sql)
    sql = raw"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'variablemappings')
    BEGIN
        CREATE TABLE [variablemappings] (
            [variablemapping_id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
            [variablemap_id] INT NOT NULL,
            [destination_id] INT NOT NULL,
            [from_id] INT NULL,
            [operator] NVARCHAR(MAX) NULL,
            [operants] NVARCHAR(MAX) NULL,
            [prerequisite_id] INT NULL,
            CONSTRAINT [fk_variablemappings_variablemap_id] FOREIGN KEY ([variablemap_id]) REFERENCES [variablemaps]([variablemap_id]),
            CONSTRAINT [fk_variablemappings_destination_id] FOREIGN KEY ([destination_id]) REFERENCES [variables]([variable_id]),
            CONSTRAINT [fk_variablemappings_from_id] FOREIGN KEY ([from_id]) REFERENCES [variables]([variable_id]),
            CONSTRAINT [fk_variablemappings_prerequisite_id] FOREIGN KEY ([prerequisite_id]) REFERENCES [variables]([variable_id])
        );
    END
    """
    DBInterface.execute(db, sql)
    return nothing
end
