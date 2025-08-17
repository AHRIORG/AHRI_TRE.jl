"""
    blake3_digest_hex(path::AbstractString) -> String

Computes the BLAKE3 digest of a file and returns it as a hexadecimal string.
- 'path' is the path to the file for which the digest is computed.
"""
function blake3_digest_hex(path::AbstractString)::String
    open(path, "r") do io
        digest_bytes = blake3sum(io)
        return lowercase(bytes2hex(digest_bytes))
    end
end
"""
    verify_blake3_digest(path::AbstractString, expected_hex::AbstractString) -> Bool

Checks whether the BLAKE3 digest of the file matches the expected hex digest.
- 'path' is the path to the file for which the digest is computed.
- 'expected_hex' is the expected hexadecimal digest string.
Returns `true` if the computed digest matches the expected digest, `false` otherwise.
"""
function verify_blake3_digest(path::AbstractString, expected_hex::AbstractString)
    digest = blake3_digest_hex(path)
    return lowercase(digest) == lowercase(expected_hex)
end
"""
    path_to_file_uri(path::AbstractString) -> String

Convert a local filesystem path (Windows or Unix) to a `file://` URI.
- Windows local path:   C:\\Users\\me\\file.txt   -> file:///C:/Users/me/file.txt
- Windows UNC path:     \\\\srv\\share\\f.txt     -> file://srv/share/f.txt
- Unix path:            /home/me/file.txt         -> file:///home/me/file.txt
"""
function path_to_file_uri(path::AbstractString)::String
    abs_path = abspath(path)

    if Sys.iswindows()
        # UNC: \\server\share\path -> file://server/share/path
        if startswith(abs_path, "\\\\")
            # Split \\server\share\rest...
            parts = split(abs_path[3:end], '\\'; limit=2)  # after leading "\\"
            isempty(parts) && throw(ArgumentError("Invalid UNC path: $path"))
            host = parts[1]
            tail = length(parts) == 2 ? parts[2] : ""
            # Build "file://host/<share/segments...>"
            # Convert backslashes to forward slashes, then escape
            tail_norm = replace(tail, '\\' => '/')
            return "file://" * host * (isempty(tail_norm) ? "" : "/" * escapeuri(tail_norm))
        else
            # Local drive path: C:\Users\me -> file:///C:/Users/me
            norm_path = replace(abs_path, '\\' => '/')
            return "file:///" * escapeuri(norm_path)
        end
    else
        # Unix-like
        return "file://" * escapeuri(abs_path)  # abs_path already starts with "/"
    end
end

"""
    file_uri_to_path(uri::AbstractString) -> String

Convert a `file://` URI to a local filesystem path (Windows or Unix).
Handles:
- file:///C:/...        -> C:\\... (Windows)
- file://server/share   -> \\\\server\\share (Windows UNC)
- file:///home/me/...   -> /home/me/... (Unix)
"""
function file_uri_to_path(uri::AbstractString)::String
    u = URI(uri)
    u.scheme == "file" || throw(ArgumentError("Not a file:// URI"))

    # Decode path portion (slashes still present)
    decoded_path = unescapeuri(u.path)  # e.g., "/C:/Users/me", "/share/f.txt", "/home/me"

    if Sys.iswindows()
        if !isempty(u.host)
            # UNC: file://server/share/dir/file -> \\server\share\dir\file
            tail = decoded_path
            # For UNC, u.path should start with "/share..." — drop the leading "/"
            if startswith(tail, "/")
                tail = tail[2:end]
            end
            return "\\\\" * u.host * "\\" * replace(tail, '/' => '\\')
        else
            # Local path: file:///C:/... (u.host empty, path like "/C:/...")
            p = decoded_path
            # Strip the single leading slash before the drive letter
            if startswith(p, "/") && occursin(r"^[A-Za-z]:", p[2:end])
                p = p[2:end]
            end
            return replace(p, '/' => '\\')
        end
    else
        # Unix-like: host should be empty or "localhost"; treat both as local
        return decoded_path
    end
end

"""
    git_commit_info(dir::AbstractString = @__DIR__; short::Bool = true, script_path::AbstractString = @__FILE__)

Return version control information about the currently executing script for use in transformations.
- `dir`: The directory to search for the git repository (default is the directory of the current script).
- `short`: Whether to return a short commit hash (default is true, returns first 7 characters of the hash).
- `script_path`: The path to the script being executed (default is the current script file).
Returns a tuple with:
- `repo_url`: The URL of the git repository (or `nothing` if not found).
- `commit`: The commit hash (or `nothing` if not found).
- `script_relpath`: The relative path of the script from the repository root (or `nothing` if not found).
This function normalizes SSH remotes to HTTPS format.
If the script is not in a git repository, it returns `nothing` for all fields.
"""
function git_commit_info(dir::AbstractString=@__DIR__; short::Bool=true, script_path::AbstractString=@__FILE__)
    # normalize ssh remotes to https
    _normalize_remote(url::AbstractString) = begin
        s = String(url)
        if startswith(s, "git@")
            if (m = match(r"^git@([^:]+):(.+?)(?:\.git)?$", s)) !== nothing
                return "https://$(m.captures[1])/$(m.captures[2])"
            end
        end
        replace(s, r"\.git$" => "")
    end

    # Discover repo root via `git`
    root = try
        readchomp(`$(Git.git()) -C $(dir) rev-parse --show-toplevel`)
    catch
        return (repo_url=nothing, commit=nothing, script_relpath=nothing)
    end

    # Current commit hash
    commit = try
        h = readchomp(`$(Git.git()) -C $(root) rev-parse HEAD`)
        short ? h[1:7] : h
    catch
        nothing
    end

    # Remote URL (origin)
    repo_url = try
        u = readchomp(`$(Git.git()) -C $(root) config --get remote.origin.url`)
        isempty(u) ? nothing : _normalize_remote(u)
    catch
        nothing
    end

    # Script relpath (relative to repo root)
    script_relpath = try
        relpath(abspath(script_path), root)
    catch
        nothing
    end

    return (repo_url=repo_url, commit=commit, script_relpath=script_relpath)
end
"""
    convert_missing_to_string!(df::DataFrame)

If the column type is Missing, convert the column eltype to Union{String, Missing}.
"""
function convert_missing_to_string!(df::DataFrame)
    for name in names(df)
        if eltype(df[!, name]) == Missing
            df[!, name] = convert(Vector{Union{String,Missing}}, df[!, name])
        end
    end
    return nothing
end
