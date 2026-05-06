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
    Trình kết nối SQL Server chuẩn hóa (Ultimate Version - Updated Explicit Commits).
    
    Features:
    - Core: Fast Executemany, Unicode (NVARCHAR), Physical Staging Tables, Explicit Commits.
    - Features: Upsert (Merge), Schema Evolution, Auto Create Table, Replace Table with PK.
    - Strategies: 'last' (Update), 'skip' (Ignore), 'sum' (Aggregate numeric).
    """

    def __init__(self, server: str, database: str, 
                        username: str, password: str, 
                        driver: str = 'ODBC Driver 17 for SQL Server', 
                        connection_type: str = 'pyodbc',
                        **kwargs):

        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
        self.connection_url = None
        self.engine = None
        if connection_type == 'pyodbc':
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
            self.engine = create_engine(
                self.connection_url,
                fast_executemany=True, 
                pool_pre_ping=True
            )

        elif connection_type == 'pymssql':
            self.connection_url = URL.create(
                "mssql+pymssql",
                username=self.username,
                password=self.password,
                host=self.server,  
                database=self.database
            )
        
            self.engine = create_engine(
                self.connection_url,
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
        """Thực thi lệnh không trả về dữ liệu (Đã thêm cơ chế Explicit Commit)"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), params or {})
                conn.commit()
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

    def _make_columns_not_null(self, target_table: str, columns: List[str], conn):
        """Helper: Chuyển các cột sang trạng thái NOT NULL để làm Primary Key hợp lệ."""
        for col in columns:
            sql = f"""
            SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{target_table}' AND COLUMN_NAME = '{col}'
            """
            result = conn.execute(text(sql)).fetchone()
            if not result:
                raise ValueError(f"Column '{col}' not found in table '{target_table}'")
            
            data_type, char_len = result[0], result[1]
            type_str = data_type.upper()
            
            if type_str in ['VARCHAR', 'NVARCHAR', 'CHAR', 'NCHAR']:
                # SQL Server giới hạn index/PK cho string tối đa thường là 900 bytes (NVARCHAR(450))
                # Nên nếu là MAX (-1) thì ta ép về 450 để tránh lỗi.
                if char_len == -1 or char_len > 450: 
                    type_str = f"{type_str}(450)"
                else:
                    type_str = f"{type_str}({char_len})"
                    
            alter_sql = f"ALTER TABLE [{target_table}] ALTER COLUMN [{col}] {type_str} NOT NULL"
            conn.execute(text(alter_sql))

    def replace_table(self, 
                      df: pd.DataFrame, 
                      target_table: str, 
                      primary_key: Union[str, List[str]] = None, 
                      add_auto_increment_id: bool = False,
                      id_column_name: str = 'id'):
        """
        Thay thế toàn bộ bảng bằng dữ liệu mới.
        """
        if df.empty:
            logger.warning(f"DataFrame is empty. Skipping replace for {target_table}.")
            return

        df = df.copy()

        # 1. Force datetime types
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

        dtype_mapping = self._generate_dtype_mapping(df)
        
        # 2. Null-safety cho pyodbc
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype(object).where(df[col].notnull(), None)

        try:
            with self.engine.connect() as conn:
                logger.info(f"Replacing table '{target_table}'...")
                
                # A. Replace toàn bộ dữ liệu bảng
                df.to_sql(
                    name=target_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    dtype=dtype_mapping
                )
                conn.commit() # ÉP COMMIT
                
                # B. Xử lý Primary Key
                if add_auto_increment_id:
                    logger.info(f"Adding auto-increment PK '{id_column_name}' to '{target_table}'...")
                    sql_add_pk = f"ALTER TABLE [{target_table}] ADD [{id_column_name}] INT IDENTITY(1,1) PRIMARY KEY;"
                    conn.execute(text(sql_add_pk))
                    conn.commit() # ÉP COMMIT
                    
                elif primary_key:
                    pk_cols = [primary_key] if isinstance(primary_key, str) else primary_key
                    logger.info(f"Setting Primary Key {pk_cols} for '{target_table}'...")
                    
                    # Cập nhật cấu trúc cột thành NOT NULL
                    self._make_columns_not_null(target_table, pk_cols, conn)
                    conn.commit() # ÉP COMMIT
                    
                    # Khởi tạo khóa chính
                    pk_str = ", ".join([f"[{c}]" for c in pk_cols])
                    constraint_name = f"PK_{target_table.replace('.','_')}_{uuid.uuid4().hex[:4]}"
                    pk_sql = f"ALTER TABLE [{target_table}] ADD CONSTRAINT {constraint_name} PRIMARY KEY ({pk_str});"
                    conn.execute(text(pk_sql))
                    conn.commit() # ÉP COMMIT

                logger.success(f"Successfully replaced table '{target_table}' ({len(df)} rows).")

        except Exception as e:
            logger.error(f"Replace table failed for {target_table}: {e}")
            raise e

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
        dtype_mapping = self._generate_dtype_mapping(df)
        
        # 5. NULL-SAFETY FOR PYODBC
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if col in join_keys:
                    df[col] = df[col].dt.normalize()
                df[col] = df[col].astype(object).where(df[col].notnull(), None)

        staging_table = f"Staging_{uuid.uuid4().hex[:10]}"

        try:
            with self.engine.connect() as conn:
                # ==========================================
                # 🔴 ĐOẠN LOG ĐIỀU TRA: KIỂM TRA ĐÍCH ĐẾN THỰC TẾ
                # ==========================================
                actual_db = conn.execute(text("SELECT DB_NAME()")).scalar()
                actual_server = conn.execute(text("SELECT @@SERVERNAME")).scalar()
                logger.info("="*50)
                logger.info(f"🕵️ ĐIỀU TRA GHI DỮ LIỆU:")
                logger.info(f"   👉 Server thực tế đang kết nối: {actual_server}")
                logger.info(f"   👉 Database thực tế đang đứng: {actual_db}")
                logger.info(f"   👉 Bảng chuẩn bị Upsert: {target_table}")
                logger.info("="*50)
                # ==========================================
                # --- A. Kiểm tra & Tạo bảng đích nếu chưa có ---
                inspector = inspect(conn)
                if not inspector.has_table(target_table):
                    logger.info(f"Table {target_table} not found. Creating new...")
                    df.to_sql(target_table, conn, index=False, dtype=dtype_mapping)
                    conn.commit() # ÉP COMMIT
                    if join_keys:
                        self._make_columns_not_null(target_table, join_keys, conn)
                        pk_str = ", ".join([f"[{c}]" for c in join_keys])
                        try:
                            conn.execute(text(f"ALTER TABLE [{target_table}] ADD CONSTRAINT PK_{target_table.replace('.','_')}_{uuid.uuid4().hex[:4]} PRIMARY KEY ({pk_str})"))
                            conn.commit() # ÉP COMMIT
                        except Exception as e:
                            logger.warning(f"Could not create PK: {e}")
                    return 

                # --- B. Schema Evolution ---
                df = df.loc[:, ~df.columns.duplicated()].copy()
                db_cols = self._get_table_columns(target_table, conn)
                df_cols = list(df.columns)
                
                db_cols_lower = [c.lower() for c in db_cols]
                new_cols = [c for c in df_cols if c.lower() not in db_cols_lower]
                
                if new_cols:
                    if auto_evolve_schema:
                        self._add_missing_columns(target_table, new_cols, dtype_mapping, conn)
                        db_cols.extend(new_cols)
                        conn.commit() # ÉP COMMIT
                    else:
                        valid_cols = [c for c in df_cols if c.lower() in db_cols_lower]
                        df = df[valid_cols]

                # --- C. Đẩy vào Staging ---
                df.to_sql(
                    name=staging_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    dtype=dtype_mapping
                )
                conn.commit() # ÉP COMMIT

                # --- D. Dynamic MERGE Logic ---
                if join_keys:
                    common_cols = [c for c in df.columns if c.lower() in [db.lower() for db in db_cols]]
                    
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
                    conn.commit() # ÉP COMMIT
                    logger.success(f"Upserted {len(df)} rows to {target_table} (Strategy: {conflict_strategy})")
                else:
                    insert_cols = ", ".join([f"[{col}]" for col in df.columns])
                    conn.execute(text(f"INSERT INTO [{target_table}] ({insert_cols}) SELECT {insert_cols} FROM [{staging_table}]"))
                    conn.commit() # ÉP COMMIT
                    logger.info(f"Appended {len(df)} rows to {target_table}")

        except Exception as e:
            logger.error(f"Upsert failed for {target_table}: {e}")
            raise e
        finally:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(f"IF OBJECT_ID('{staging_table}', 'U') IS NOT NULL DROP TABLE [{staging_table}]"))
                    conn.commit() # ÉP COMMIT
            except Exception:
                pass

    def delete_and_insert(self, 
                          df: pd.DataFrame, 
                          target_table: str, 
                          delete_keys: Union[str, List[str]], 
                          auto_evolve_schema: bool = True):
        """
        Chiến lược Idempotent: Xóa dữ liệu cũ dựa trên các khóa (delete_keys) rồi Insert dữ liệu mới.
        """
        if df.empty:
            logger.warning(f"DataFrame is empty. Skipping delete_and_insert for {target_table}.")
            return

        df = df.copy()

        # 1. Ép kiểu Datetime để đồng bộ
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

        keys = [delete_keys] if isinstance(delete_keys, str) else delete_keys
        dtype_mapping = self._generate_dtype_mapping(df)

        # 2. Trích xuất các giá trị unique để làm điều kiện DELETE 
        delete_conditions = {}
        for key in keys:
            if key not in df.columns:
                raise ValueError(f"Delete key '{key}' không tồn tại trong DataFrame.")
            
            unique_vals = df[key].dropna().unique()
            if len(unique_vals) > 0:
                delete_conditions[key] = unique_vals
                
        # 3. NULL-SAFETY cho PyODBC
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if col in keys:
                    df[col] = df[col].dt.normalize()
                df[col] = df[col].astype(object).where(df[col].notnull(), None)

        try:
            with self.engine.connect() as conn:
                # --- A. Kiểm tra & Tạo bảng đích nếu chưa có ---
                inspector = inspect(conn)
                if not inspector.has_table(target_table):
                    logger.info(f"Table {target_table} not found. Creating new and inserting data...")
                    df.to_sql(target_table, conn, index=False, dtype=dtype_mapping)
                    conn.commit() # ÉP COMMIT
                    logger.success(f"Successfully inserted {len(df)} rows to new table '{target_table}'.")
                    return 

                # --- B. Schema Evolution ---
                df = df.loc[:, ~df.columns.duplicated()].copy()
                db_cols = self._get_table_columns(target_table, conn)
                df_cols = list(df.columns)
                
                db_cols_lower = [c.lower() for c in db_cols]
                new_cols = [c for c in df_cols if c.lower() not in db_cols_lower]
                
                if new_cols:
                    if auto_evolve_schema:
                        self._add_missing_columns(target_table, new_cols, dtype_mapping, conn)
                        db_cols.extend(new_cols)
                        conn.commit() # ÉP COMMIT
                    else:
                        valid_cols = [c for c in df_cols if c.lower() in db_cols_lower]
                        df = df[valid_cols]

                # --- C. XÓA DỮ LIỆU CŨ (DELETE) ---
                deleted_rows_approx = 0
                for key, vals in delete_conditions.items():
                    formatted_vals = []
                    for v in vals:
                        if isinstance(v, (int, float, np.integer, np.floating)):
                            formatted_vals.append(str(v))
                        else:
                            safe_str = str(v).replace("'", "''") 
                            formatted_vals.append(f"'{safe_str}'")
                            
                    in_clause = ", ".join(formatted_vals)
                    
                    if in_clause:
                        delete_sql = f"DELETE FROM [{target_table}] WHERE [{key}] IN ({in_clause})"
                        result = conn.execute(text(delete_sql))
                        deleted_rows_approx += result.rowcount
                        
                if deleted_rows_approx > 0:
                    logger.info(f"Deleted {deleted_rows_approx} old rows based on keys {keys}.")
                    conn.commit() # ÉP COMMIT

                # --- D. THÊM DỮ LIỆU MỚI (INSERT) ---
                df.to_sql(
                    name=target_table,
                    con=conn,
                    if_exists='append', 
                    index=False,
                    dtype=dtype_mapping
                )
                conn.commit() # ÉP COMMIT
                logger.success(f"Idempotent Insert: Successfully appended {len(df)} rows to '{target_table}'.")

        except Exception as e:
            logger.error(f"Delete and Insert failed for {target_table}: {e}")
            raise e

    def dispose(self):
        self.engine.dispose()