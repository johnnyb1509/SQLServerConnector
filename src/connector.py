import os
import pandas as pd
import numpy as np
import uuid
import sqlalchemy
from typing import List, Optional, Dict, Union, Any, Literal
from loguru import logger
from sqlalchemy import create_engine, text, URL, inspect
from sqlalchemy.types import NVARCHAR, FLOAT, INTEGER, DATE, DATETIME, BIGINT

class SQLServerConnector:
    """
    Trình kết nối SQL Server chuẩn hóa (Full Features - Fixed Missing Attribute).
    Tích hợp:
    - Fast Executemany (Tốc độ cao).
    - Unicode Support (NVARCHAR).
    - Upsert Strategy (Last/Skip).
    - Schema Evolution.
    - Helper methods: check_table_exists.
    """

    def __init__(self, server: str, database: str, username: str, password: str, driver: str = 'ODBC Driver 17 for SQL Server', **kwargs):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
        # Tạo URL kết nối
        self.connection_url = URL.create(
            "mssql+pyodbc",
            query={
                "odbc_connect": (
                    f"DRIVER={self.driver};"
                    f"SERVER={self.server};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    "Encrypt=no;TrustServerCertificate=yes;"
                )
            }
        )
        
        # Engine với fast_executemany=True
        self.engine = create_engine(
            self.connection_url,
            fast_executemany=True, 
            pool_pre_ping=True
        )

    def get_data(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Thực thi SELECT và trả về DataFrame"""
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(text(query), conn, params=params)
        except Exception as e:
            logger.error(f"Get data error: {e}")
            raise e

    def execute_query(self, query: str, params: Optional[Dict] = None):
        """Thực thi lệnh không trả về dữ liệu (UPDATE, DELETE, etc.)"""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query), params or {})
        except Exception as e:
            logger.error(f"Execute query error: {e}")
            raise e

    # --- [ĐÃ BỔ SUNG LẠI HÀM NÀY] ---
    def check_table_exists(self, table_name: str) -> bool:
        """Kiểm tra bảng có tồn tại trong database không"""
        try:
            inspector = inspect(self.engine)
            return inspector.has_table(table_name)
        except Exception as e:
            logger.error(f"Check table exists failed: {e}")
            return False
    # --------------------------------

    def _generate_dtype_mapping(self, df: pd.DataFrame) -> Dict:
        """Tự động map kiểu dữ liệu (NVARCHAR cho string)"""
        dtype_map = {}
        for col in df.columns:
            if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                dtype_map[col] = NVARCHAR(length=None)
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                dtype_map[col] = DATETIME()
            elif pd.api.types.is_float_dtype(df[col]):
                dtype_map[col] = FLOAT()
            elif pd.api.types.is_integer_dtype(df[col]):
                dtype_map[col] = BIGINT()
        return dtype_map

    def _get_table_columns(self, table_name: str, conn) -> List[str]:
        """Lấy danh sách cột hiện có trong DB"""
        inspector = inspect(conn)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        return columns

    def _add_missing_columns(self, table_name: str, missing_cols: List[str], dtype_map: Dict, conn):
        """Alter table để thêm cột thiếu (Schema Evolution)"""
        for col in missing_cols:
            col_type = dtype_map.get(col, NVARCHAR(255))
            # SQLAlchemy type to string conversion logic simplified
            type_str = "NVARCHAR(MAX)" # Default safe fallback
            if isinstance(col_type, FLOAT): type_str = "FLOAT"
            elif isinstance(col_type, BIGINT): type_str = "BIGINT"
            elif isinstance(col_type, DATETIME): type_str = "DATETIME"
            elif isinstance(col_type, DATE): type_str = "DATE"
            
            alter_sql = f"ALTER TABLE [{table_name}] ADD [{col}] {type_str}"
            conn.execute(text(alter_sql))
            logger.info(f"Auto-evolve: Added column '{col}' to table '{table_name}'")

    def upsert_data(self, 
                    df: pd.DataFrame, 
                    target_table: str, 
                    match_columns: List[str], 
                    conflict_strategy: Literal['last', 'skip'] = 'last',
                    auto_evolve_schema: bool = False):
        """
        Hàm Upsert mạnh mẽ.
        
        Args:
            df: DataFrame cần upload.
            target_table: Tên bảng đích.
            match_columns: Danh sách cột dùng làm Key so khớp (Primary Key).
            conflict_strategy: 
                - 'last': Update ghi đè dữ liệu mới vào dòng cũ (Default).
                - 'skip': Nếu trùng key thì bỏ qua, không update.
            auto_evolve_schema: 
                - True: Tự động thêm cột vào DB nếu DF có cột mới.
                - False: Bỏ qua các cột trong DF mà DB không có (Strict Schema).
        """
        if df.empty:
            logger.warning(f"DataFrame for {target_table} is empty. Skip.")
            return

        # 1. Map Unicode Types
        dtype_mapping = self._generate_dtype_mapping(df)
        
        # 2. Staging Table Name
        staging_table = f"##Staging_{uuid.uuid4().hex[:8]}"

        try:
            with self.engine.begin() as conn:
                # --- A. Kiểm tra Schema & Table ---
                inspector = inspect(conn)
                if not inspector.has_table(target_table):
                    logger.info(f"Table {target_table} not found. Creating new...")
                    df.to_sql(target_table, conn, index=False, dtype=dtype_mapping)
                    # Tạo Primary Key nếu cần
                    if match_columns:
                        pk_str = ", ".join([f"[{c}]" for c in match_columns])
                        try:
                            conn.execute(text(f"ALTER TABLE [{target_table}] ADD CONSTRAINT PK_{target_table.replace('.','_')}_{uuid.uuid4().hex[:4]} PRIMARY KEY ({pk_str})"))
                        except Exception as e:
                            logger.warning(f"Could not create PK: {e}")
                    return

                # --- B. Xử lý Schema Evolution ---
                db_cols = self._get_table_columns(target_table, conn)
                df_cols = list(df.columns)
                
                # Tìm cột có trong DF mà không có trong DB
                new_cols = [c for c in df_cols if c not in db_cols]
                
                if new_cols:
                    if auto_evolve_schema:
                        self._add_missing_columns(target_table, new_cols, dtype_mapping, conn)
                        db_cols.extend(new_cols) # Update danh sách cột DB
                    else:
                        # Nếu không auto evolve, chỉ giữ lại các cột khớp với DB
                        valid_cols = [c for c in df_cols if c in db_cols]
                        if len(valid_cols) < len(df_cols):
                            logger.warning(f"Schema strict: Dropping columns {new_cols} because they are not in DB.")
                            df = df[valid_cols]
                
                # --- C. Đẩy vào Staging (Fast Executemany) ---
                df.to_sql(
                    name=staging_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    dtype=dtype_mapping
                )

                # --- D. Thực hiện MERGE ---
                # Chỉ lấy các cột chung giữa DF và DB để Merge (tránh lỗi cột không tồn tại)
                common_cols = [c for c in df.columns if c in db_cols]
                
                on_clause = " AND ".join([f"Target.[{col}] = Source.[{col}]" for col in match_columns])
                
                # Logic Insert
                insert_cols = ", ".join([f"[{col}]" for col in common_cols])
                insert_vals = ", ".join([f"Source.[{col}]" for col in common_cols])

                # Logic Update
                merge_sql = ""
                
                # Trường hợp 1: Update ('last')
                if conflict_strategy == 'last':
                    update_cols = [c for c in common_cols if c not in match_columns]
                    if update_cols:
                        update_set = ", ".join([f"Target.[{col}] = Source.[{col}]" for col in update_cols])
                        merge_sql = f"""
                        MERGE [{target_table}] AS Target
                        USING {staging_table} AS Source
                        ON {on_clause}
                        WHEN MATCHED THEN
                            UPDATE SET {update_set}
                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT ({insert_cols}) VALUES ({insert_vals});
                        """
                    else:
                        # Nếu chỉ có cột PK, không có gì để update -> Chỉ Insert if not exists
                        merge_sql = f"""
                        MERGE [{target_table}] AS Target
                        USING {staging_table} AS Source
                        ON {on_clause}
                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT ({insert_cols}) VALUES ({insert_vals});
                        """
                
                # Trường hợp 2: Skip (Chỉ Insert, không Update)
                elif conflict_strategy == 'skip':
                    merge_sql = f"""
                    MERGE [{target_table}] AS Target
                    USING {staging_table} AS Source
                    ON {on_clause}
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({insert_cols}) VALUES ({insert_vals});
                    """

                conn.execute(text(merge_sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
                logger.info(f"Upserted {len(df)} rows to {target_table} (Strategy: {conflict_strategy})")

        except Exception as e:
            logger.error(f"Upsert failed for {target_table}: {e}")
            raise e

    def dispose(self):
        self.engine.dispose()