using DataFrames
using ConfigEnv
using Git

dotenv()

# Return (repo_url, commit) for the repo containing `dir` (default: this file’s dir)
# - repo_url: normalized HTTPS URL if possible, else the raw remote URL; nothing if unavailable
# - commit: short (7) or full 40-char hash; nothing if not in a git repo
function git_commit_info(dir::AbstractString = @__DIR__; short::Bool = true)
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
        return (repo_url = nothing, commit = nothing)
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

    return (repo_url = repo_url, commit = commit)
end
# ...existing code...
# Example usage
info = git_commit_info()
if info.repo_url !== nothing && info.commit !== nothing
    println("Repository URL: $(info.repo_url)")
    println("Commit: $(info.commit)")
else
    println("Not in a git repository.")
end
