#!/bin/bash
set -e

echo "Running post-create setup..."

# Install Julia packages once the General registry is available
julia --project -e 'using Pkg; isempty(Pkg.Registry.reachable_registries()) && Pkg.Registry.add("General"); Pkg.resolve(); Pkg.instantiate()'

echo "Post-create setup completed successfully!"
