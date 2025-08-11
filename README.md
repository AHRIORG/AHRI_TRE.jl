# AHRI Trusted Research Environment

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://kobusherbst.github.io/RDAIngest.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://kobusherbst.github.io/RDAIngest.jl/dev/)

Julia code to manage the AHRI TRE

The TRE uses a PostgreSQL database to store metadata. The data lake is based on duckdb and ducklake and is used to store the actual data. Ducklake uses the PostgreSQL database to store metadata about the data stored in the data lake.

1. Create a new datastore, including the ducklake based data lake
2. Open a a datastore
3. Add a new study to a datastore
4. Add a domain to the datastore
5. Add entities and entity relationships to the datastore
6. Add variables to the datastore
   a. Read variables from a REDCap project using the REDCap API