#!/bin/bash
export AIRFLOW__API__AUTH_BACKEND='airflow.api.auth.backend.basic_auth'
export AIRFLOW__CORE__LOAD_EXAMPLES='false'
airflow standalone
