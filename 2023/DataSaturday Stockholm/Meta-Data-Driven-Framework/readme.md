**Meta-Data-Driven-Framework**


This is just a small readme as a start. Any questions please ask me. I will try to update the readme as soon as possible

Create a Serverless database in Synapse and 

**Add the following schema's:**

IF NOT EXISTS ( SELECT 1 FROM sys.schemas WHERE name = 'Current' ) EXEC ('CREATE SCHEMA [Current]')

IF NOT EXISTS ( SELECT 1 FROM sys.schemas WHERE name = 'Historical' ) EXEC ('CREATE SCHEMA [Historical]')

IF NOT EXISTS ( SELECT 1 FROM sys.schemas WHERE name = 'Execution' ) EXEC ('CREATE SCHEMA [Execution]')

IF NOT EXISTS ( SELECT 1 FROM sys.schemas WHERE name = 'Deltalake' ) EXEC ('CREATE SCHEMA [Deltalake]')


**Add the following stored procedures:**

CreateLakehousePersistTables.sql
CreateLakehouseTables.sql

Restore the Database


Add the following linked services, details are available in the folder PipelineTemplate

LS_ASQL_CONFIG_FRAMEWORK
LS_ASQL_SOURCE
LS_ASA_SERVERLESS
LS_AKV_KEYVAULT
LS_ADLS_DLS2

**Secrets**
ASQL-METADATAFRAMEWORK  ConnectionString to your Framework database
ASA-OXGN01-LAKEHOUSE-META-DATA  ConnectionString to your Synapse Lakehouse serverless database
ADLS-DLS2  Accountkey to your ADLS Gen 2 account