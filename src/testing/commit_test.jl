using AHRI_TRE

commit_info = AHRI_TRE.git_commit_info()
@info "Commit Info: Repository URL=$(commit_info.repo_url), Commit Hash=$(commit_info.commit), File Path=$(commit_info.script_relpath)"