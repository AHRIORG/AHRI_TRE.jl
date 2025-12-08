#!/bin/bash
set -e

echo "Running post-create setup..."

# Install Julia packages once the General registry is available
julia --project -e 'using Pkg; isempty(Pkg.Registry.reachable_registries()) && Pkg.Registry.add("General"); Pkg.resolve(); Pkg.instantiate()'

# Configure Julia startup with Revise (avoid indenting heredoc)
mkdir -p /root/.julia/config
cat > /root/.julia/config/startup.jl <<'EOF'
try
    using Revise
catch err
    @warn "Revise failed to load" err
end
EOF

echo "Post-create setup completed successfully!"
