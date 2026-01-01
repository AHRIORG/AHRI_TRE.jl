# meta_msql.jl - MSSQL-specific SQL metadata extraction overrides
#region MSSQL Connection

"""
    find_system_odbc_driver(driver_name::AbstractString) -> Union{String,Nothing}

Search system ODBC configuration files for the specified driver and return its path.
Checks /etc/odbcinst.ini on Linux and /opt/homebrew/etc/odbcinst.ini on macOS.
"""
function find_system_odbc_driver(driver_name::AbstractString)::Union{String,Nothing}
    config_paths = if Sys.islinux()
        ["/etc/odbcinst.ini"]
    elseif Sys.isapple()
        ["/opt/homebrew/etc/odbcinst.ini", "/usr/local/etc/odbcinst.ini", "/etc/odbcinst.ini"]
    else
        String[]
    end
    
    for config_path in config_paths
        if !isfile(config_path)
            continue
        end
        
        try
            content = read(config_path, String)
            lines = split(content, '\n')
            
            # Find the driver section
            in_section = false
            for line in lines
                line = strip(line)
                
                # Check if we're entering the driver section
                if startswith(line, '[') && endswith(line, ']')
                    section_name = strip(line[2:end-1])
                    in_section = (section_name == driver_name)
                    continue
                end
                
                # If in the correct section, look for Driver= line
                if in_section && startswith(line, "Driver")
                    parts = split(line, '=', limit=2)
                    if length(parts) == 2
                        driver_path = strip(parts[2])
                        if isfile(driver_path)
                            return driver_path
                        end
                    end
                end
            end
        catch e
            @debug "Failed to read $config_path" exception=e
        end
    end
    
    return nothing
end

"""
    find_mssql_driver_in_directory() -> Union{String,Nothing}

Scan common installation directories for Microsoft ODBC Driver libraries.
Returns the path to the first valid driver found, or nothing if none found.
"""
function find_mssql_driver_in_directory()::Union{String,Nothing}
    search_dirs = if Sys.islinux()
        [
            "/opt/microsoft/msodbcsql18/lib64",
            "/opt/microsoft/msodbcsql17/lib64",
            "/opt/microsoft/msodbcsql/lib64",
        ]
    elseif Sys.isapple()
        [
            "/opt/homebrew/lib",
            "/usr/local/lib",
        ]
    else
        String[]
    end
    
    driver_pattern = if Sys.islinux()
        r"libmsodbcsql-\d+\.\d+\.so"
    elseif Sys.isapple()
        r"libmsodbcsql\.\d+\.dylib"
    else
        r""
    end
    
    for dir in search_dirs
        if !isdir(dir)
            continue
        end
        
        try
            files = readdir(dir, join=true)
            # Filter for driver libraries and sort by version (newest first)
            driver_files = filter(f -> isfile(f) && occursin(driver_pattern, basename(f)), files)
            sort!(driver_files, rev=true)  # Newest version first
            
            if !isempty(driver_files)
                return first(driver_files)
            end
        catch e
            @debug "Failed to scan directory $dir" exception=e
        end
    end
    
    return nothing
end

"""
    ensure_mssql_driver_registered(driver_name::AbstractString="ODBC Driver 18 for SQL Server")

Ensure the MSSQL ODBC driver is registered with ODBC.jl. On Linux/macOS systems,
this checks if the driver is registered and attempts to register it from system
locations if not found.

The function searches for drivers in the following order:
1. Already registered with ODBC.jl
2. System ODBC configuration files (/etc/odbcinst.ini)
3. Common installation directories

Returns `true` if driver is available, `false` otherwise.
"""
function ensure_mssql_driver_registered(driver_name::AbstractString="ODBC Driver 18 for SQL Server")::Bool
    # Check if driver already registered
    drivers = ODBC.drivers()
    if haskey(drivers, driver_name)
        return true
    end
    
    # Windows uses system driver manager, no registration needed
    if Sys.iswindows()
        return true
    end
    
    # Try to find driver path from system configuration
    driver_path = find_system_odbc_driver(driver_name)
    
    # If not in system config, search installation directories
    if isnothing(driver_path)
        driver_path = find_mssql_driver_in_directory()
    end
    
    # Register the driver if found
    if !isnothing(driver_path)
        try
            ODBC.adddriver(driver_name, driver_path)
            @info "Registered MSSQL ODBC driver: $driver_name at $driver_path"
            return true
        catch e
            @error "Failed to register driver from $driver_path" exception=e
            return false
        end
    end
    
    @warn "MSSQL ODBC driver '$driver_name' not found. Install Microsoft ODBC Driver for SQL Server."
    return false
end

"""
    connect_mssql(server::AbstractString, database::AbstractString,
                  user::AbstractString, password::AbstractString;
                  driver::AbstractString="ODBC Driver 18 for SQL Server",
                  encrypt::Bool=true, trust_server_cert::Bool=true) -> Union{ODBC.Connection,Nothing}

Create a connection to a Microsoft SQL Server database using ODBC.

# Arguments
- `server`: The server hostname or IP address (e.g., "myserver.database.windows.net")
- `database`: The database name to connect to
- `user`: The username for authentication
- `password`: The password for authentication
- `driver`: ODBC driver name (default: "ODBC Driver 18 for SQL Server")
- `encrypt`: Whether to use encrypted connection (default: true)
- `trust_server_cert`: Whether to trust the server certificate (default: true)

# Returns
An `ODBC.Connection` object, or `nothing` if connection fails.

# Note
This function automatically registers the MSSQL ODBC driver with ODBC.jl if not already
registered, searching common system installation paths.

# Example
```julia
conn = connect_mssql("myserver.database.windows.net", "mydb", "user", "password")
try
    result = DBInterface.execute(conn, "SELECT @@VERSION") |> DataFrame
    println(result)
finally
    DBInterface.close!(conn)
end
```
"""
function connect_mssql(server::AbstractString, database::AbstractString,
    user::AbstractString, password::AbstractString;
    driver::AbstractString="ODBC Driver 18 for SQL Server",
    encrypt::Bool=true, trust_server_cert::Bool=true)::Union{ODBC.Connection,Nothing}
    try
        # Ensure driver is registered
        if !ensure_mssql_driver_registered(driver)
            @error "MSSQL ODBC driver not available: $driver"
            return nothing
        end
        
        # On macOS, Microsoft ODBC driver requires unixODBC
        if Sys.isapple()
            ODBC.setunixODBC()
        end
        
        connStr = ""
        if Sys.iswindows()
            connStr = "Driver={$driver};SERVER=$(server);DATABASE=$database;Trusted_Connection=yes;TrustServerCertificate=$(trust_server_cert ? "yes" : "no");AutoTranslate=no"
        elseif Sys.islinux() || Sys.isapple()
            connStr = "Driver={$driver};Server=$(server);Database=$database;UID=$(user);PWD=$(password);TrustServerCertificate=$(trust_server_cert ? "yes" : "no");AutoTranslate=no;"
        else
            error("Unsupported OS for MSSQL connection")
        end
        return ODBC.Connection(connStr)
    catch err
        @error "Failed to connect to SQL Server $server, database $database" exception = err
        return nothing
    end
end
#endregion

#region Type Mapping
# MSSQL-specific type mapping
# Handles system_type_name format from dm_exec_describe_first_result_set
# which includes precision/scale like "varchar(50)", "decimal(18,2)", "datetime2(7)"
function map_sql_type_to_tre(sql_type::AbstractString, ::MSSQLFlavour)::Int
    type_lower = lowercase(sql_type)

    # Strip parenthetical precision/scale for matching: "varchar(50)" -> "varchar"
    base_type = replace(type_lower, r"\s*\([^)]*\)\s*$" => "")

    # Integer types
    if base_type in ["int", "integer", "smallint", "bigint", "tinyint", "bit"]
        return TRE_TYPE_INTEGER
    end

    # Float types
    if base_type in ["float", "real", "decimal", "numeric", "money", "smallmoney"]
        return TRE_TYPE_FLOAT
    end

    # DateTime types (check before date to avoid substring match)
    if base_type in ["datetime", "datetime2", "datetimeoffset", "smalldatetime"]
        return TRE_TYPE_DATETIME
    end

    # Date types
    if base_type == "date"
        return TRE_TYPE_DATE
    end

    # Time types
    if base_type == "time"
        return TRE_TYPE_TIME
    end

    # String types (default)
    return TRE_TYPE_STRING
end
#endregion

#region Column Metadata Extraction
"""
    get_query_columns(conn, sql::AbstractString, ::MSSQLFlavour) -> Vector{ColumnInfo}

MSSQL-specific implementation using sys.dm_exec_describe_first_result_set to get
rich column metadata including source table and schema information.
"""
function get_query_columns(conn, sql::AbstractString, ::MSSQLFlavour)::Vector{ColumnInfo}
    # Use sys.dm_exec_describe_first_result_set to get detailed column metadata
    meta_sql = """
    SELECT
        name,
        system_type_name,
        is_nullable,
        source_table,
        source_schema,
        source_column
    FROM sys.dm_exec_describe_first_result_set(?, NULL, 1)
    WHERE is_hidden = 0
    ORDER BY column_ordinal
    """

    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, meta_sql, [sql]) |> DataFrame
        for row in eachrow(result)
            col_name = String(row[:name])
            data_type = String(row[:system_type_name])
            is_nullable = row[:is_nullable] == true || row[:is_nullable] == 1

            # Source table and schema may be NULL if column is computed/derived
            table_name = if !ismissing(row[:source_table]) && !isnothing(row[:source_table])
                String(row[:source_table])
            else
                nothing
            end
            schema_name = if !ismissing(row[:source_schema]) && !isnothing(row[:source_schema])
                String(row[:source_schema])
            else
                nothing
            end

            push!(columns, ColumnInfo(col_name, data_type, is_nullable, table_name, schema_name))
        end
    catch e
        # Fallback to generic approach if dm_exec_describe_first_result_set fails
        @warn "MSSQL dm_exec_describe_first_result_set failed, falling back to generic approach: $e"
        wrapped_sql = "SELECT TOP 0 * FROM ($sql) AS _meta_query"
        result = DBInterface.execute(conn, wrapped_sql)
        df = DataFrame(result)

        for col_name in names(df)
            col_type = eltype(df[!, col_name])
            sql_type = julia_type_to_sql_string(col_type)
            push!(columns, ColumnInfo(String(col_name), sql_type, col_type >: Missing, nothing, nothing))
        end
    end
    return columns
end

function wrap_query_for_metadata(sql::AbstractString, ::MSSQLFlavour)::String
    # MSSQL uses TOP instead of LIMIT
    return "SELECT TOP 0 * FROM ($sql) AS _meta_query"
end
#endregion

#region Column Comments
function get_column_comment(conn, table_name::AbstractString, column_name::AbstractString,
                           schema_name::Union{AbstractString,Nothing}, ::MSSQLFlavour)::Union{String,Missing}
    schema = isnothing(schema_name) ? "dbo" : schema_name
    sql = """
    SELECT CAST(ep.value AS VARCHAR(MAX)) AS comment
    FROM sys.extended_properties ep
    JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = ? AND c.name = ? AND s.name = ? AND ep.name = 'MS_Description'
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name, schema]) |> DataFrame
        if nrow(result) > 0 && !ismissing(result[1, :comment])
            return String(result[1, :comment])
        end
    catch
        # Column comment not available
    end
    return missing
end
#endregion

#region Constraints / Code Tables
function get_foreign_key_reference(conn, table_name::AbstractString, column_name::AbstractString,
                                   ::MSSQLFlavour)::Union{Nothing,Tuple{String,String}}
    sql = """
    SELECT TOP 1
        rt.name AS referenced_table,
        rc.name AS referenced_column
    FROM sys.foreign_key_columns fkc
    JOIN sys.tables pt ON fkc.parent_object_id = pt.object_id
    JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
    JOIN sys.tables rt ON fkc.referenced_object_id = rt.object_id
    JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
    WHERE pt.name = ?
      AND pc.name = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        if nrow(result) > 0
            return (String(result[1, :referenced_table]), String(result[1, :referenced_column]))
        end
    catch
    end
    return nothing
end

function table_has_primary_key(conn, table_name::AbstractString, column_name::AbstractString, ::MSSQLFlavour)::Bool
    sql = """
    SELECT COUNT(*) as cnt
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND tc.TABLE_NAME = ?
      AND kcu.COLUMN_NAME = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function get_check_constraint_values(conn, table_name::AbstractString, column_name::AbstractString,
                                     ::MSSQLFlavour)::Vector{VocabularyItem}
    sql = """
    SELECT cc.definition as constraint_def
    FROM sys.check_constraints cc
    JOIN sys.tables t ON cc.parent_object_id = t.object_id
    JOIN sys.columns c ON cc.parent_object_id = c.object_id AND cc.parent_column_id = c.column_id
    WHERE t.name = ? AND c.name = ?
    """
    items = VocabularyItem[]
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        for row in eachrow(result)
            constraint_def = String(row[:constraint_def])
            items = vcat(items, parse_check_constraint_values(constraint_def, column_name))
        end
    catch
    end
    return items
end
#endregion

#region Quoting / Table Inspection
function quote_identifier(name::AbstractString, ::MSSQLFlavour)::String
    return "[$name]"
end

function table_exists(conn, table_name::AbstractString, ::MSSQLFlavour)::Bool
    sql = "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?"
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        return result[1, :cnt] > 0
    catch
        return false
    end
end

function get_original_column_type(conn, table_name::AbstractString, column_name::AbstractString,
                                  ::MSSQLFlavour)::Union{String,Nothing}
    sql = """
    SELECT DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
    """
    try
        result = DBInterface.execute(conn, sql, [table_name, column_name]) |> DataFrame
        if nrow(result) > 0
            return String(result[1, :DATA_TYPE])
        end
    catch
    end
    return nothing
end

function get_table_columns(conn, table_name::AbstractString, ::MSSQLFlavour)::Vector{ColumnInfo}
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE, CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    columns = ColumnInfo[]
    try
        result = DBInterface.execute(conn, sql, [table_name]) |> DataFrame
        for row in eachrow(result)
            push!(columns, ColumnInfo(
                String(row[:COLUMN_NAME]),
                String(row[:DATA_TYPE]),
                row[:is_nullable] == 1,
                table_name,
                nothing
            ))
        end
    catch
    end
    return columns
end
#endregion
