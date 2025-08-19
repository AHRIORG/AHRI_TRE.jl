# AHRI Trusted Research Environment

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://ahriorg.github.io/AHRI_TRE.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://ahriorg.github.io/AHRI_TRE.jl/dev/)

Julia code to manage the AHRI TRE

The TRE uses a PostgreSQL database to store metadata. The data lake is based on duckdb and ducklake and is used to store the actual data. Ducklake uses the PostgreSQL database to store metadata about the data stored in the data lake.
