import os
import numpy as np
import pandas as pd
import uuid
from typing import List, Optional, Dict, Union, Any
from loguru import logger
from sqlalchemy import create_engine, inspect, text, URL
from sqlalchemy.types import NVARCHAR, FLOAT, INTEGER, DATE, DATETIME, BIGINT
from sqlalchemy.exc import SQLAlchemyError

class SQLServerConnector:
    """
    A robust, SQLAlchemy 2.0 compliant connector for SQL Server designed for ETL processes.
    
    Features:
    - High-performance Upserts (Merge) using Unique Staging Tables.
    - Advanced Conflict Resolution: 'sum' (for finance) or 'last' (for metadata).
    - Automatic Schema Evolution and Primary Key management.
    - Unicode/Vietnamese support (NVARCHAR + UTF8).
    """

    def __init__(self, server: str, database: str, username: str, password: str, driver: str = 'ODBC Driver 17 for SQL Server'):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
        self.connection_url = URL.create(
            "mssql+pyodbc",
            query={
                "odbc_connect": (
                    f"DRIVER={self.driver};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    "Charsets=UTF-8;"
                ),
                "fast_executemany": "True" 
            }
        )
        
        self.engine = create_engine(
            self.connection_url, 
            pool_pre_ping=True,  
            pool_size=20, 
            max_overflow=10
        )
        
    def dispose(self):
        self.engine.dispose()
        logger.info("Database engine disposed.")

    # ========================================================
    # SCHEMA HELPERS
    # ========================================================

    def check_table_exists(self, table_name: str) -> bool:
        return inspect(self.engine).has_table(table_name)

    def get_columns_info(self, table_name: str) -> Dict[str, str]:
        inspector = inspect(self.engine)
        return {col['name']: str(col['type']) for col in inspector.get_columns(table_name)}

    # ========================================================
    # CORE ETL METHODS
    # ========================================================

    def upsert_data(self, df: pd.DataFrame, target_table: str, primary_key: str = None, 
                    match_columns: Optional[List[str]] = None, auto_evolve_schema: bool = True,
                    conflict_strategy: str = 'sum'):
        if df.empty: return

        join_keys = match_columns if match_columns else ([primary_key] if primary_key else [])
        
        # 1. Sanitize & lọc lấy các cột cần thiết
        # Chỉ giữ lại join_keys và các cột có dữ liệu để tránh "phân mảnh" dữ liệu khi gộp
        df_clean = self._sanitize_dataframe(df, exclude_cols=join_keys)
        
        # 2. Xử lý trùng lặp triệt để
        initial_len = len(df_clean)
        if conflict_strategy == 'sum':
            # Xác định cột số để cộng dồn
            num_cols = df_clean.select_dtypes(include=[np.number]).columns.tolist()
            num_cols = [c for c in num_cols if c not in join_keys]
            
            # Chỉ gộp trên các cột số, các cột text khác key sẽ bị loại bỏ hoặc lấy dòng đầu
            # Điều này đảm bảo kết quả trả về CHỈ CÓ 1 DÒNG cho mỗi cặp Key
            agg_logic = {col: 'sum' for col in num_cols}
            
            # Đối với các cột không phải số và không phải key, chúng ta lấy dòng đầu tiên
            other_cols = [c for c in df_clean.columns if c not in join_keys and c not in num_cols]
            for c in other_cols:
                agg_logic[c] = 'first'
                
            df_clean = df_clean.groupby(join_keys, as_index=False).agg(agg_logic)
        else:
            df_clean = df_clean.drop_duplicates(subset=join_keys, keep='last')

        if len(df_clean) < initial_len:
            logger.info(f"Conflict Resolution ({conflict_strategy}): Combined {initial_len} -> {len(df_clean)} rows.")

        # 3. Schema Management
        if not self.check_table_exists(target_table):
            self._create_table_from_df(df_clean, target_table, primary_key)
        elif auto_evolve_schema:
            self._sync_columns(df_clean, target_table)

        # 4. Execute Merge
        self._execute_merge_upsert(df_clean, target_table, join_keys)

    def _execute_merge_upsert(self, df: pd.DataFrame, target_table: str, join_keys: List[str]):
        # Use a unique staging name to support parallel tasks
        unique_id = str(uuid.uuid4()).replace('-', '')[:10]
        staging_table = f"##stg_{unique_id}_{target_table[:20]}"
        
        with self.engine.begin() as conn: 
            try:
                # Explicit mapping for Unicode
                dtype_map = {col: NVARCHAR(None) for col in df.columns if df[col].dtype == 'object'}
                df.to_sql(staging_table, conn, if_exists='replace', index=False, dtype=dtype_map)
                
                source_cols = list(df.columns)
                on_clause = " AND ".join([f"Target.[{k}] = Source.[{k}]" for k in join_keys])
                update_stmts = [f"Target.[{col}] = Source.[{col}]" for col in source_cols if col not in join_keys]
                
                insert_cols = ", ".join([f"[{col}]" for col in source_cols])
                insert_vals = ", ".join([f"Source.[{col}]" for col in source_cols])
                
                sql = f"""
                MERGE [{target_table}] AS Target USING [{staging_table}] AS Source
                ON ({on_clause})
                {f"WHEN MATCHED THEN UPDATE SET {', '.join(update_stmts)}" if update_stmts else ""}
                WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});
                """
                conn.execute(text(sql))
                conn.execute(text(f"DROP TABLE [{staging_table}]"))
                logger.success(f"Successfully upserted {len(df)} rows to {target_table}.")
            except Exception as e:
                logger.error(f"Merge execution failed for {target_table}: {e}")
                raise

    # ========================================================
    # UTILS: CLEANING & SCHEMA
    # ========================================================

    def _sanitize_dataframe(self, df: pd.DataFrame, exclude_cols: List[str]) -> pd.DataFrame:
        df = df.copy()
        # Clean Dates
        for col in df.select_dtypes(include=['datetime']).columns:
            df[col] = df[col].replace({pd.NaT: None})
        # Clean NaN/None
        df = df.replace({np.nan: None})
        df = df.where(pd.notnull(df), None)
        return df

    def _create_table_from_df(self, df: pd.DataFrame, table_name: str, primary_key: Optional[str]):
        dtype_map = {col: NVARCHAR(None) for col in df.columns if df[col].dtype == 'object'}
        df.to_sql(table_name, self.engine, index=False, dtype=dtype_map)
        if primary_key and primary_key in df.columns:
            self.set_primary_key(table_name, primary_key, df[primary_key].dtype)

    def set_primary_key(self, table_name: str, column_name: str, source_dtype):
        sql_type = "NVARCHAR(450)" if pd.api.types.is_string_dtype(source_dtype) else "BIGINT"
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"ALTER TABLE [{table_name}] ALTER COLUMN [{column_name}] {sql_type} NOT NULL"))
                conn.execute(text(f"ALTER TABLE [{table_name}] ADD PRIMARY KEY ([{column_name}])"))

    def _sync_columns(self, df: pd.DataFrame, table_name: str):
        db_cols = {k.lower() for k in self.get_columns_info(table_name).keys()}
        new_cols = [c for c in df.columns if c.lower() not in db_cols]
        
        if new_cols:
            with self.engine.connect() as conn:
                for col in new_cols:
                    sql_type = "NVARCHAR(MAX)" if df[col].dtype == 'object' else "FLOAT"
                    conn.execute(text(f"ALTER TABLE [{table_name}] ADD [{col}] {sql_type} NULL"))
                conn.commit()

    # ========================================================
    # DATA RETRIEVAL METHODS
    # ========================================================
    def get_data(self,  query: str, params: Optional[Dict[str, Any]] = None, chunksize: Optional[int] = None) -> Union[pd.DataFrame, Any]:
        """
        Executes a SQL query and returns a Pandas DataFrame.
        
        Args:
            query (str): The SQL query string. Use :param_name for parameters.
            params (dict, optional): Dictionary of parameters to bind to the query.
            chunksize (int, optional): If specified, returns an iterator where each chunk is the given size.
        
        Returns:
            pd.DataFrame or Iterator[pd.DataFrame]
        """
        try:
            with self.engine.connect() as conn:
                # Use text() explicitly for SQLAlchemy 2.0 compatibility
                sql_query = text(query)
                
                # If chunksize is provided, read_sql returns a generator
                result = pd.read_sql(
                    sql_query, 
                    conn, 
                    params=params, 
                    chunksize=chunksize
                )
                
                if chunksize is None:
                    logger.info(f"Successfully retrieved {len(result)} rows.")
                else:
                    logger.info(f"Retrieving data in chunks of {chunksize} rows.")
                    
                return result
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve data: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during get_data: {e}")
            raise