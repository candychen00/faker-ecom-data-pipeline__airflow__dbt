FROM python:3.12-slim

RUN pip install dbt-bigquery

COPY . /my_dbt

WORKDIR /my_dbt/my_dbt_project

ENV DBT_PROFILES_DIR=/my_dbt/my_dbt_project

RUN dbt deps




