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
    Trình kết nối SQL Server chuẩn hóa (Ultimate Version).
    
    Features:
    - Core: Fast Executemany, Unicode (NVARCHAR), Physical Staging Tables.
    - Features: Upsert (Merge), Schema Evolution, Auto Create Table.
    - Strategies: 'last' (Update), 'skip' (Ignore), 'sum' (Aggregate numeric).
    """

    def __init__(self, server: str, database: str, username: str, password: str, driver: str = 'ODBC Driver 17 for SQL Server', **kwargs):
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
        """Thực thi lệnh không trả về dữ liệu"""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(query), params or {})
        except Exception as e:
            logger.error(f"Execute query error: {e}")
            raise e

    def check_table_exists(self, table_name: str) -> bool:
        """Kiểm tra bảng có tồn tại không"""
        try:
            inspector = inspect(self.engine)
            return inspector.has_table(table_name)
        except Exception as e:
            logger.error(f"Check table exists failed: {e}")
            return False

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
        """Alter table để thêm cột thiếu"""
        for col in missing_cols:
            col_type = dtype_map.get(col, NVARCHAR(255))
            type_str = "NVARCHAR(MAX)"
            if isinstance(col_type, FLOAT): type_str = "FLOAT"
            elif isinstance(col_type, BIGINT): type_str = "BIGINT"
            elif isinstance(col_type, DATETIME): type_str = "DATETIME"
            elif isinstance(col_type, DATE): type_str = "DATE"
            
            conn.execute(text(f"ALTER TABLE [{table_name}] ADD [{col}] {type_str}"))
            logger.info(f"Auto-evolve: Added column '{col}' to table '{table_name}'")

    def upsert_data(self, 
                    df: pd.DataFrame, 
                    target_table: str, 
                    primary_key: Union[str, List[str]] = None, 
                    match_columns: Optional[List[str]] = None, 
                    auto_evolve_schema: bool = True,
                    conflict_strategy: Literal['sum', 'last', 'skip'] = 'last'):
        
        if df.empty:
            return

        df = df.copy()

        # 1. FORCE DATES TO DATETIME64 EARLY
        for col in df.columns:
            if df[col].dtype == 'object':
                idx = df[col].first_valid_index()
                if idx is not None:
                    val = df[col].loc[idx]
                    if hasattr(val, 'year') and hasattr(val, 'month') and hasattr(val, 'day'):
                        try:
                            df[col] = pd.to_datetime(df[col])
                        except Exception:
                            pass

        # 2. Xác định Join Keys
        join_keys = match_columns or (
            [primary_key] if isinstance(primary_key, str) else primary_key
        ) or []

        if not join_keys:
             logger.warning(f"No keys provided for {target_table}. Switching to APPEND mode.")
        else:
            # 3. Aggregation & Deduplication
            initial_count = len(df)
            if conflict_strategy == 'sum':
                num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                num_cols = [c for c in num_cols if c not in join_keys]
                
                agg_logic = {col: 'sum' for col in num_cols}
                other_cols = [c for c in df.columns if c not in join_keys and c not in num_cols]
                for c in other_cols:
                    agg_logic[c] = 'last'
                    
                df = df.groupby(join_keys, as_index=False).agg(agg_logic)
                if len(df) < initial_count:
                    logger.info(f"Strategy 'sum': Aggregated {initial_count} -> {len(df)} rows.")
            else:
                keep_val = 'last' if conflict_strategy == 'last' else 'first'
                df = df.drop_duplicates(subset=join_keys, keep=keep_val).copy()
                if len(df) < initial_count:
                    logger.warning(f"Dropped {initial_count - len(df)} duplicate rows based on keys {join_keys}.")

        # 4. GENERATE DTYPE MAPPING *BEFORE* NULL-SAFETY CASTING
        # Because df['date'] is datetime64 right now, it correctly maps to DATETIME()
        dtype_mapping = self._generate_dtype_mapping(df)
        
        # 5. NULL-SAFETY FOR PYODBC
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if col in join_keys:
                    df[col] = df[col].dt.normalize()
                # This makes the column an 'object' holding None, but the mapping is already saved!
                df[col] = df[col].astype(object).where(df[col].notnull(), None)

        staging_table = f"Staging_{uuid.uuid4().hex[:10]}"

        try:
            with self.engine.begin() as conn:
                # --- A. Kiểm tra & Tạo bảng đích nếu chưa có ---
                inspector = inspect(conn)
                if not inspector.has_table(target_table):
                    logger.info(f"Table {target_table} not found. Creating new...")
                    df.to_sql(target_table, conn, index=False, dtype=dtype_mapping)
                    if join_keys:
                        pk_str = ", ".join([f"[{c}]" for c in join_keys])
                        try:
                            conn.execute(text(f"ALTER TABLE [{target_table}] ADD CONSTRAINT PK_{target_table.replace('.','_')}_{uuid.uuid4().hex[:4]} PRIMARY KEY ({pk_str})"))
                        except Exception as e:
                            logger.warning(f"Could not create PK: {e}")
                    return 

                # --- B. Schema Evolution ---
                db_cols = self._get_table_columns(target_table, conn)
                df_cols = list(df.columns)
                new_cols = [c for c in df_cols if c not in db_cols]
                
                if new_cols:
                    if auto_evolve_schema:
                        self._add_missing_columns(target_table, new_cols, dtype_mapping, conn)
                        db_cols.extend(new_cols)
                    else:
                        valid_cols = [c for c in df_cols if c in db_cols]
                        df = df[valid_cols]

                # --- C. Đẩy vào Staging ---
                df.to_sql(
                    name=staging_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    dtype=dtype_mapping
                )

                # --- D. Dynamic MERGE Logic ---
                if join_keys:
                    common_cols = [c for c in df.columns if c in db_cols]
                    
                    on_conditions = []
                    for col in join_keys:
                        on_conditions.append(f"(Target.[{col}] = Source.[{col}] OR (Target.[{col}] IS NULL AND Source.[{col}] IS NULL))")
                    on_clause = " AND ".join(on_conditions)
                    
                    insert_cols = ", ".join([f"[{col}]" for col in common_cols])
                    insert_vals = ", ".join([f"Source.[{col}]" for col in common_cols])
                    
                    merge_target = f"[{target_table}] WITH (HOLDLOCK)"

                    merge_sql = ""
                    if conflict_strategy in ['last', 'sum']:
                        update_cols = [c for c in common_cols if c not in join_keys]
                        if update_cols:
                            update_set = ", ".join([f"Target.[{col}] = Source.[{col}]" for col in update_cols])
                            merge_sql = f"""
                            MERGE {merge_target} AS Target USING [{staging_table}] AS Source
                            ON {on_clause}
                            WHEN MATCHED THEN UPDATE SET {update_set}
                            WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});
                            """
                        else:
                            merge_sql = f"""
                            MERGE {merge_target} AS Target USING [{staging_table}] AS Source
                            ON {on_clause}
                            WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});
                            """
                    elif conflict_strategy == 'skip':
                        merge_sql = f"""
                        MERGE {merge_target} AS Target USING [{staging_table}] AS Source
                        ON {on_clause}
                        WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals});
                        """

                    conn.execute(text(merge_sql))
                    logger.success(f"Upserted {len(df)} rows to {target_table} (Strategy: {conflict_strategy})")
                else:
                    insert_cols = ", ".join([f"[{col}]" for col in df.columns])
                    conn.execute(text(f"INSERT INTO [{target_table}] ({insert_cols}) SELECT {insert_cols} FROM [{staging_table}]"))
                    logger.info(f"Appended {len(df)} rows to {target_table}")

        except Exception as e:
            logger.error(f"Upsert failed for {target_table}: {e}")
            raise e
        finally:
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL DROP TABLE [{staging_table}]"))
            except Exception:
                pass

    def dispose(self):
        self.engine.dispose()