# Bank Platform Work in progress

## Overview
A Databricks notebook that generates realistic, deliberately messy data from
 retail-banking source systems into a Unity Catalog Volume, for use with
Auto Loader and streaming pipelines.

## Data quality challenges included
Duplicates, out-of-order and late-arriving events, deletes, schema evolution,
amount mismatches, PII in free text, fraud bursts, account-takeover sessions.

## Tools & Technologies
Databricks, PySpark, Unity Catalog Volumes, Delta Lake, Auto Loader, Excel

## Data
Simulates 10 source systems: 
core banking CDC (customers and accounts),
card transactions,
Visa/Mastercard clearing files,
fraud labels,
FX rates,
sanctions watchlists,
CRM contacts and cases,
mobile clickstream.
