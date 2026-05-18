#!/usr/bin/env python3
"""
Central Database Manager & Schema Migrator
Provides a unified connection pipeline and SQL-file-based schema DDL execution.
"""

import os
from pathlib import Path
import psycopg2
from loguru import logger
from dotenv import load_dotenv

# Ensure environment variables are loaded
load_dotenv()

# Central DDL schema file location
SCHEMA_FILE_PATH = Path(__file__).resolve().parent.parent / "config" / "schema.sql"

def get_db_connection() -> psycopg2.extensions.connection:
    """
    Establish a connection to PostgreSQL using settings parsed from .env.
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", 5432))
    dbname = os.getenv("POSTGRES_DB", "testdb")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    logger.debug(f"Connecting to database '{dbname}' on {host}:{port}...")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def init_db_schema(conn: psycopg2.extensions.connection) -> None:
    """
    Load the unified resources/database/schema.sql file and apply it 
    directly to the target database.
    """
    if not SCHEMA_FILE_PATH.exists():
        logger.error(f"Centralized DDL schema file not found at: {SCHEMA_FILE_PATH}")
        raise FileNotFoundError(f"Schema file not found: {SCHEMA_FILE_PATH}")

    logger.info(f"Applying unified DDL schema from {SCHEMA_FILE_PATH}...")
    try:
        with open(SCHEMA_FILE_PATH, "r", encoding="utf-8") as f:
            schema_ddl = f.read()

        with conn.cursor() as cursor:
            # Execute the entire SQL script
            cursor.execute(schema_ddl)
            conn.commit()
            
        logger.info("Successfully synchronized database schema with centralized DDL.")
    except Exception as e:
        conn.rollback()
        logger.exception(f"Failed to initialize database schema: {e}")
        raise

def drop_db_schema(conn: psycopg2.extensions.connection) -> None:
    """
    Drop all core tables to allow clean setup or reset.
    """
    logger.warning("Dropping core database schemas (subjects, patient_events)...")
    try:
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS patient_events CASCADE;")
            cursor.execute("DROP TABLE IF EXISTS subjects CASCADE;")
            conn.commit()
        logger.info("Successfully dropped core database schemas.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to drop core database schemas: {e}")
        raise
