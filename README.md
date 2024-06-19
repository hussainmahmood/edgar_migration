## Description

This repository contains code that was used to create a pipeline for migrating some relations from [Securities and Exchange Commission](https://www.sec.gov/edgar/search-and-access) PostgreSQL database to a local MSSQL Server database.

Some of these relations had JSON columns which needed to be transformed before data could be migrated to MSSQL Server as SQL Server doesn't support storing JSON data directly.

It is just for educational purposes, we do not take responsibility for any illicit use.

![Securities and Exchange Commission](edgar.png)