"""
    sha256_digest_hex(path::AbstractString) -> String

Compute the SHA-256 (SHA2-256) digest of a file and return it as a lowercase hex string.
- `path`: Path to the file to hash.
"""
function sha256_digest_hex(path::AbstractString)::String
    open(path, "r") do io
        digest_bytes = SHA.sha256(io)
        return lowercase(bytes2hex(digest_bytes))
    end
end

"""
    verify_sha256_digest(path::AbstractString, expected_hex::AbstractString) -> Bool

Check whether the file's SHA-256 digest matches the expected hex string.
Returns `true` on match, `false` otherwise.
"""
function verify_sha256_digest(path::AbstractString, expected_hex::AbstractString)
    digest = sha256_digest_hex(path)
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
    decoded_path = nothing
    if u.path == ""
        decoded_path = unescapeuri(u.host)
    else
        decoded_path = unescapeuri(u.path)
    end
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
    _normalize_remote(url::AbstractString)

Normalize a git remote URL by converting SSH format to HTTPS format.
"""
_normalize_remote(url::AbstractString) = begin
    s = String(url)
    if startswith(s, "git@")
        if (m = match(r"^git@([^:]+):(.+?)(?:\.git)?$", s)) !== nothing
            return "https://$(m.captures[1])/$(m.captures[2])"
        end
    end
    replace(s, r"\.git$" => "")
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

    # Discover repo root via `git`
    root = try
        readchomp(`$(Git.git()) -C $(dir) rev-parse --show-toplevel`)
    catch
        return (repo_url=missing, commit=missing, script_relpath=missing)
    end
    @info "Git repository root detected at: $root" 
    # Current commit hash
    commit = try
        h = readchomp(`$(Git.git()) -C $(root) rev-parse HEAD`)
        short ? String(h[1:7]) : String(h)
    catch
        missing
    end
    @info "Current commit hash: $(commit === missing ? "not found" : commit)"
    # Remote URL (origin)
    repo_url = try
        u = readchomp(`$(Git.git()) -C $(root) config --get remote.origin.url`)
        isempty(u) ? missing : _normalize_remote(u)
    catch
        missing
    end
    @info "Repository URL: $(repo_url === missing ? "not found" : repo_url)"
    # Script relpath (relative to repo root)
    script_relpath = try
        relpath(abspath(script_path), root)
    catch
        missing
    end
    @info "Script relative path in repo: $(script_relpath === missing ? "not found" : script_relpath)"
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
#region NCName conversion
_is_start_char(c::Char) = occursin(_RE_START, string(c))
_is_name_char(c::Char, strict::Bool=false) = strict ? occursin(_RE_STRICT, string(c)) : occursin(_RE_NAME, string(c))

"""
    is_ncname(s::AbstractString) -> Bool

Return true if `s` is a valid NCName (no colon, proper start char, allowed name chars).
"""
function is_ncname(s::AbstractString; strict::Bool=false)
    isempty(s) && return false
    occursin(':', s) && return false
    it = iterate(s)
    it === nothing && return false
    (c, st) = it
    _is_start_char(c) || return false
    while true
        it = iterate(s, st)
        it === nothing && break
        (c, st) = it
        _is_name_char(c, strict) || return false
    end
    return true
end

"""
    to_ncname(s::AbstractString; replacement="_", prefix="_", avoid_reserved=true) -> String

Convert `s` into a valid NCName:

- Replaces any invalid char with `replacement` (default `_`).
- If the first char is invalid, prepends `prefix` (default `_`).
- Removes/condenses repeated `replacement`s.
- Replaces `:` with `replacement`.
- Optionally avoids names starting with 'xml' (case-insensitive) by prepending `prefix`.
- If strict=true, disallows `-` and `.` in names (replaces them too).
"""
function to_ncname(s::AbstractString; replacement="_", prefix="_", avoid_reserved::Bool=true, strict::Bool=false)
    t = strip(s)
    # Replace colons outright
    if !isempty(replacement)
        t = replace(t, ':' => replacement)
    else
        t = replace(t, ":" => "")
    end

    # Build sanitized name
    io = IOBuffer()
    it = iterate(t)
    if it === nothing
        return prefix * "x"
    end

    (c, st) = it
    if _is_start_char(c)
        print(io, c)
    else
        print(io, prefix)
        print(io, _is_name_char(c, strict) ? c : replacement)
    end

    while true
        it = iterate(t, st)
        it === nothing && break
        (c, st) = it
        print(io, _is_name_char(c, strict) ? c : replacement)
    end

    out = String(take!(io))

    # Collapse multiple replacements (e.g., "___" -> "_")
    if !isempty(replacement)
        rep = replace(replacement, r"([\\.^$|?*+()\[\]{}])" => s"\\\1")  # escape for Regex
        out = replace(out, Regex("$(rep){2,}") => replacement)
    end

    # Ensure not empty and not starting with reserved 'xml' (if requested)
    isempty(out) && (out = prefix * "x")
    if avoid_reserved && startswith(lowercase(out), "xml")
        out = prefix * out
    end

    return out
end
#endregion NCName conversion
"""
    emptydir(path; create=true, retries=3, wait=0.2)

Ensure `path` exists (if `create=true`), then remove all files/subdirs inside it,
keeping `path` itself. Retries briefly to tolerate transient files.

Safety: refuses to operate on the filesystem root "/".
"""
function emptydir(path::AbstractString; create::Bool=true, retries::Integer=3, wait::Real=0.2)
    path = abspath(path)
    real = try
        realpath(path)
    catch
        path
    end
    real == "/" && error("Refusing to operate on root '/'")

    if !isdir(path)
        create || error("Directory does not exist: $path")
        mkpath(path)
    end

    for attempt in 1:retries
        # delete files first, then dirs (bottom-up)
        for (root, dirs, files) in walkdir(path; topdown=false)
            for f in files
                p = joinpath(root, f)
                try
                    rm(p; force=true)
                catch
                    # ignore; retry next pass
                end
            end
            for d in dirs
                p = joinpath(root, d)
                try
                    rm(p; recursive=false, force=true)
                catch
                    # ignore; retry next pass
                end
            end
        end

        isempty(readdir(path)) && return
        sleep(wait)
    end

    # Final hard pass with explicit error reporting
    try
        for entry in readdir(path; join=true)
            rm(entry; recursive=true, force=true)
        end
    catch e
        error("Final cleanup of $path failed: $(typeof(e)): $(e.msg)")
    end

    if !isempty(readdir(path))
        error("Failed to empty $path: some entries remain (likely permission issues or open file handles).")
    end
end
"""
    caller_file_runtime()

Return the file path of the script that called this function at runtime.
  'level' indicates how many levels up the call stack to go (default 1 = immediate caller).
"""
function caller_file_runtime(level::Int=1)
    for (i, fr) in enumerate(stacktrace())
        # skip this helper frame
        if i > level
            return String(fr.file)
        end
    end
    return nothing
end