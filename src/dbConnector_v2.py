import os
import numpy as np
import pandas as pd
import yaml
from typing import List, Optional, Dict, Union, Any
from loguru import logger
from sqlalchemy import create_engine, inspect, text, URL
from sqlalchemy.types import NVARCHAR, FLOAT, INTEGER, DATE, DATETIME, BIGINT
from sqlalchemy.exc import SQLAlchemyError

# ========================================================
# SQL SERVER CONNECTOR (Standardized ETL Object)
# Tech Stack: SQLAlchemy 2.0+, Pandas, PyODBC
# Unicode Support: YES (Vietnamese/UTF-8)
# ========================================================

class SQLServerConnector:
    """
    A robust, SQLAlchemy 2.0 compliant connector for SQL Server designed for ETL processes.
    
    Features:
    - High-performance Upserts (Merge) using Staging Tables.
    - Full Unicode/Vietnamese Support (NVARCHAR + UTF8).
    - Automatic Schema Evolution (adds missing columns).
    - Automatic Primary Key detection and creation.
    """

    def __init__(self, server: str, database: str, username: str, password: str, driver: str = 'ODBC Driver 17 for SQL Server'):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
        # Connection URL Construction
        # CRITICAL: 'fast_executemany' is required for proper Unicode handling in bulk inserts with PyODBC
        self.connection_url = URL.create(
            "mssql+pyodbc",
            query={
                "odbc_connect": (
                    f"DRIVER={self.driver};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    "Charsets=UTF-8;"  # Explicitly request UTF-8
                ),
                "fast_executemany": "True" 
            }
        )
        
        # Create Engine
        self.engine = create_engine(
            self.connection_url, 
            pool_pre_ping=True,  
            pool_size=20, 
            max_overflow=10
        )
        
    def get_engine(self):
        """Returns the SQLAlchemy engine object."""
        return self.engine

    def close(self):
        """Alias for dispose(). Closes all connections in the pool."""
        self.dispose()

    def dispose(self):
        """Dispose of the engine and close all connections."""
        self.engine.dispose()
        logger.info("Database engine disposed and connections closed.")

    # ========================================================
    # SCHEMA & METADATA METHODS
    # ========================================================

    def get_table_names(self) -> List[str]:
        """Retrieve all table names in the database."""
        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names()
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving table names: {e}")
            return []

    def check_table_exists(self, table_name: str) -> bool:
        """Check if a specific table exists."""
        return inspect(self.engine).has_table(table_name)

    def get_primary_key(self, table_name: str) -> Optional[str]:
        """Retrieve the primary key column name for a table."""
        try:
            inspector = inspect(self.engine)
            pk_constraint = inspector.get_pk_constraint(table_name)
            if pk_constraint and pk_constraint['constrained_columns']:
                return pk_constraint['constrained_columns'][0]
            
            # Fallback heuristic
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            for candidate in ['id_date', 'id', 'ID', 'Date', 'date']:
                if candidate in columns:
                    return candidate
            return None
        except Exception as e:
            logger.warning(f"Could not inspect PK for {table_name}: {e}")
            return None

    def get_columns_info(self, table_name: str) -> Dict[str, str]:
        """Get column names and their SQL types."""
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        return {col['name']: str(col['type']) for col in columns}

    # ========================================================
    # DATA RETRIEVAL METHODS
    # ========================================================

    def get_data(self, query_or_table: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a raw SQL query or fetch a whole table.
        Args:
            query_or_table: SQL Query string OR Table Name.
            params: Dictionary of parameters for the query.
        """
        if "SELECT" not in query_or_table.upper() and " " not in query_or_table:
            query = text(f"SELECT * FROM {query_or_table}")
        else:
            query = text(query_or_table)
            
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return pd.DataFrame()

    # ========================================================
    # CORE ETL METHODS (Upsert Logic)
    # ========================================================

    def upsert_data(self, 
                    df: pd.DataFrame, 
                    target_table: str, 
                    primary_key: str = 'id_date', 
                    match_columns: Optional[List[str]] = None,
                    auto_evolve_schema: bool = True):
        """
        Main ETL Function with Unicode Support.
        
        Args:
            df: The new data to push.
            target_table: The SQL table name.
            primary_key: The Database Primary Key.
            match_columns: Columns to match on (e.g. ['Ticker', 'Date']) for detecting updates.
            auto_evolve_schema: If True, adds missing columns to SQL automatically.
        """
        if df.empty:
            logger.warning(f"Dataframe for {target_table} is empty. Skipping.")
            return

        # 1. PRE-PROCESS DATA
        df_clean = self._sanitize_dataframe(df)
        
        # 2. CHECK TARGET TABLE
        if not self.check_table_exists(target_table):
            logger.info(f"Table {target_table} does not exist. Creating new table.")
            self._create_table_from_df(df_clean, target_table, primary_key)
            return

        # 3. SCHEMA EVOLUTION
        if auto_evolve_schema:
            self._sync_columns(df_clean, target_table)

        # 4. DETERMINE MATCHING LOGIC
        if match_columns:
            join_keys = match_columns
        elif primary_key in df_clean.columns:
            join_keys = [primary_key]
        else:
            logger.error(f"CRITICAL: Primary Key '{primary_key}' is missing from DataFrame (likely Auto-Increment).")
            logger.error("You MUST provide 'match_columns' to identify which rows to update.")
            raise ValueError("Missing match keys for Identity Column Upsert.")

        # 5. EXECUTE UPSERT VIA STAGING
        self._execute_merge_upsert(df_clean, target_table, join_keys)

    def _execute_merge_upsert(self, df: pd.DataFrame, target_table: str, join_keys: List[str]):
        """Internal: Uploads to a temp table and runs a SQL MERGE."""
        staging_table = f"##staging_{target_table}"
        
        with self.engine.begin() as conn: 
            try:
                # A. Upload to Staging
                # IMPORTANT: We use NVARCHAR mapping implicitly here via pandas to_sql, 
                # but explicit dtype mapping is safer for Unicode preservation.
                dtype_map = {}
                for col in df.columns:
                    if df[col].dtype == 'object':
                        dtype_map[col] = NVARCHAR(None)  # Force Unicode (NVARCHAR) for all strings
                
                df.to_sql(staging_table, conn, if_exists='replace', index=False, chunksize=5000, dtype=dtype_map)
                
                # B. Build Dynamic SQL
                source_cols = [col for col in df.columns] 
                
                # Join Condition: Target.Key = Source.Key AND ...
                on_clause = " AND ".join([f"Target.[{k}] = Source.[{k}]" for k in join_keys])
                
                # Update Clause
                update_stmts = [f"Target.[{col}] = Source.[{col}]" for col in source_cols 
                                if col not in join_keys]
                
                # Insert Clause
                insert_cols_str = ", ".join([f"[{col}]" for col in source_cols])
                insert_vals_str = ", ".join([f"Source.[{col}]" for col in source_cols])
                
                # C. Construct MERGE Query
                # Notice the N prefix is usually for literals, but since we are copying column-to-column 
                # from a staging table that is ALREADY NVARCHAR, we don't need N'' prefixes here.
                if not update_stmts:
                    merge_query = f"""
                    MERGE [{target_table}] AS Target
                    USING [{staging_table}] AS Source
                    ON ({on_clause})
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({insert_cols_str}) VALUES ({insert_vals_str});
                    """
                else:
                    merge_query = f"""
                    MERGE [{target_table}] AS Target
                    USING [{staging_table}] AS Source
                    ON ({on_clause})
                    WHEN MATCHED THEN
                        UPDATE SET {", ".join(update_stmts)}
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({insert_cols_str}) VALUES ({insert_vals_str});
                    """
                
                conn.execute(text(merge_query))
                logger.success(f"Upsert successful for {target_table}. Matched on {join_keys}.")
                
                conn.execute(text(f"DROP TABLE [{staging_table}]"))
                
            except SQLAlchemyError as e:
                logger.error(f"Upsert failed for {target_table}: {e}")
                raise

    # ========================================================
    # HELPER: SCHEMA & CREATION
    # ========================================================

    def _sync_columns(self, df: pd.DataFrame, table_name: str):
        """Add missing columns to the SQL table."""
        db_cols = self.get_columns_info(table_name)
        existing_cols_lower = {k.lower() for k in db_cols.keys()}
        
        new_cols = [col for col in df.columns if col.lower() not in existing_cols_lower]
        
        if new_cols:
            logger.info(f"Schema Evolution: Adding {len(new_cols)} new columns to {table_name}.")
            with self.engine.connect() as conn:
                for col in new_cols:
                    dtype = df[col].dtype
                    # VIETNAMESE SUPPORT: Default to NVARCHAR(MAX) for new string columns
                    sql_type = "NVARCHAR(MAX)"
                    
                    if pd.api.types.is_integer_dtype(dtype):
                        sql_type = "BIGINT"
                    elif pd.api.types.is_float_dtype(dtype):
                        sql_type = "FLOAT"
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        sql_type = "DATETIME"
                    
                    try:
                        conn.execute(text(f"ALTER TABLE [{table_name}] ADD [{col}] {sql_type} NULL"))
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"Failed to add column {col}: {e}")

    def _create_table_from_df(self, df: pd.DataFrame, table_name: str, primary_key: Optional[str] = None):
        """Create a new table with Unicode support (NVARCHAR)."""
        dtype_map = {}
        for col in df.columns:
            # VIETNAMESE SUPPORT: Explicitly map all object columns to NVARCHAR
            if df[col].dtype == 'object':
                dtype_map[col] = NVARCHAR(None) # None = MAX
        
        df.to_sql(table_name, self.engine, index=False, dtype=dtype_map)
        
        if primary_key:
            if primary_key in df.columns:
                pk_dtype = df[primary_key].dtype
                self.set_primary_key(table_name, primary_key, source_dtype=pk_dtype)
            else:
                logger.warning(f"Skipping PK creation: Column '{primary_key}' not found in new data.")

    def set_primary_key(self, table_name: str, column_name: str, source_dtype=None):
        """Sets a primary key with type detection."""
        sql_type = "INT"
        if source_dtype is not None:
            if pd.api.types.is_integer_dtype(source_dtype):
                sql_type = "BIGINT"
            elif pd.api.types.is_float_dtype(source_dtype):
                sql_type = "BIGINT" 
            elif pd.api.types.is_string_dtype(source_dtype):
                # VIETNAMESE SUPPORT: PKs that are strings must also be NVARCHAR
                sql_type = "NVARCHAR(450)"
            elif pd.api.types.is_datetime64_any_dtype(source_dtype):
                sql_type = "DATE"

        with self.engine.connect() as conn:
            with conn.begin():
                try:
                    conn.execute(text(f"ALTER TABLE [{table_name}] ALTER COLUMN [{column_name}] {sql_type} NOT NULL"))
                    conn.execute(text(f"ALTER TABLE [{table_name}] ADD PRIMARY KEY ([{column_name}])"))
                    logger.info(f"Primary key set on {table_name}.{column_name}")
                except SQLAlchemyError as e:
                    logger.error(f"Failed to set PK on {table_name}: {e}")

    # ========================================================
    # HELPER: DATA CLEANING
    # ========================================================

    def _sanitize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans numeric strings, NaT, and NaN values."""
        df = df.copy()
        
        # 1. Clean Numeric Strings
        for col in df.select_dtypes(include=['object']).columns:
            # Only try to convert to float if it looks like a number (digits or currency symbols)
            # Avoid trying to convert Vietnamese text columns
            sample = df[col].dropna().head(10).astype(str).tolist()
            if any(any(char.isdigit() for char in str(x)) for x in sample):
                 try:
                     # Attempt conversion, but strictly ignore errors so we don't break text columns
                     # We use a temp series to check if conversion is successful for majority
                     temp = df[col].apply(self._clean_numeric_string)
                     # If the column was actually text (e.g., "Hà Nội"), _clean_numeric_string returns the original string.
                     # We trust _clean_numeric_string to be safe.
                     df[col] = temp
                 except:
                     pass 

        # 2. Clean Dates (NaT -> None)
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].replace({pd.NaT: None})
            df[col] = df[col].astype(object).where(df[col].notnull(), None)

        # 3. Clean NaN -> None
        df = df.replace({np.nan: None})
        df = df.where(pd.notnull(df), None)
        return df

    @staticmethod
    def _clean_numeric_string(value):
        """Convert '2.5B', '1,000' to float. Safe for Vietnamese text."""
        if pd.isna(value) or value is None: return None
        if isinstance(value, (int, float)): return value
            
        s = str(value).strip().upper()
        if not s: return None
        
        # Heuristic: If it contains many letters (excluding K,M,B,T for billions), it's probably text
        # Count letters
        alpha_count = sum(c.isalpha() for c in s)
        if alpha_count > 1 and s[-1] not in ['K', 'M', 'B', 'T']:
            return value # It's likely text (e.g. "Cổ phiếu")

        # Clean common financial chars
        s_clean = s.replace(',', '').replace('%', '')
            
        multipliers = {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}
        if s_clean and s_clean[-1] in multipliers:
            try:
                return float(s_clean[:-1]) * multipliers[s_clean[-1]]
            except ValueError:
                return value
            
        try:
            return float(s_clean)
        except ValueError:
            return value

# ========================================================
# ENTRY POINT
# ========================================================

def get_db_connector(yaml_path: Optional[str] = None, env_prefix: str = "DB") -> SQLServerConnector:
    """Factory function to initialize connector."""
    if yaml_path and os.path.exists(yaml_path):
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f).get('db_info', {})
            return SQLServerConnector(
                config.get('server'), 
                config.get('database'), 
                config.get('username'), 
                config.get('password')
            )
    else:
        return SQLServerConnector(
            os.environ.get(f'{env_prefix}_SERVER'),
            os.environ.get(f'{env_prefix}_NAME'),
            os.environ.get(f'{env_prefix}_USER'),
            os.environ.get(f'{env_prefix}_PASS')
        )