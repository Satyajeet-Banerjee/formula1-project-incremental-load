-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Set-up the project environment for formula1 project**<br>
-- MAGIC 1.Create External Location databricks-ext-datalake-formula1-incr<br>
-- MAGIC 2.Create Catalog formula1-incr<br>
-- MAGIC 3.Create Schemas landing, bronze, silver and gold<br>
-- MAGIC 4.Create Volume Files in the landing schema.

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS formula1_incr_ext_loc
URL 'abfss://formula1-incr@stformula1datalake.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL formula1_credential)
COMMENT 'External location for Formula1-incr project';

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@stformula1datalake.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Catalog**

-- COMMAND ----------

CREATE CATALOG  IF NOT EXISTS  formula1_incr
   MANAGED LOCATION 'abfss://formula1-incr@stformula1datalake.dfs.core.windows.net/' 
    COMMENT 'This is the main catalog for the formula1 project' ;

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Schemas Landing, Bronze, Silver and Gold**

-- COMMAND ----------

CREATE SCHEMA  IF NOT EXISTS  formula1_incr.landing;
CREATE SCHEMA  IF NOT EXISTS  formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@stformula1datalake.dfs.core.windows.net/bronze';
CREATE SCHEMA  IF NOT EXISTS  formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@stformula1datalake.dfs.core.windows.net/silver';
CREATE SCHEMA  IF NOT EXISTS  formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@stformula1datalake.dfs.core.windows.net/gold';        
    

-- COMMAND ----------

USE CATALOG formula1_incr;
SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Volume Files**

-- COMMAND ----------

--- Create an external volume under the directory “my-path”
CREATE EXTERNAL VOLUME IF NOT EXISTS formula1_incr.landing.files
    LOCATION 'abfss://formula1-incr@stformula1datalake.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/files