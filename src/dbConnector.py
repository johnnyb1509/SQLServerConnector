import numpy as np
import pandas as pd
import os
import sqlalchemy
import yaml
from loguru import logger
from sqlalchemy import create_engine, inspect, MetaData, text, types
from sqlalchemy.types import NVARCHAR
from sqlalchemy.engine import URL
from typing import Optional, List, Dict, Any, Union
from decimal import Decimal

# ========================================================
# DATABASE CONNECTOR MODULE
# Hỗ trợ kết nối MS SQL Server, xử lý Transaction an toàn
# và tối ưu hóa việc Insert/Update dữ liệu lớn.
# ========================================================

class dbJob:
    def __init__(self, server, database, username, password, driver='{ODBC Driver 17 for SQL Server}'):
        """
        Khởi tạo kết nối đến SQL Server.
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        
        # Khởi tạo Engine và lấy danh sách bảng hiện có
        self.engine = self.sqlServerConnect()
        self.tableInDB = self.getTableNameInDB()

    def sqlServerConnect(self):
        """Tạo SQLAlchemy engine với connection string chuẩn cho MS SQL."""
        connection_string = (
            f"DRIVER={self.driver};SERVER={self.server};"
            f"DATABASE={self.database};UID={self.username};PWD={self.password}"
        )
        connection_url = URL.create(
            "mssql+pyodbc", 
            query={"odbc_connect": connection_string, 'charset': 'utf8mb4'}
        )
        
        # fast_executemany=True giúp tăng tốc độ insert gấp nhiều lần với pyodbc
        engine = create_engine(
            connection_url, 
            pool_pre_ping=True, 
            pool_size=20, 
            max_overflow=10,
            fast_executemany=True 
        )
        return engine
    
    def getTableNameInDB(self) -> List[str]:
        """Lấy danh sách tất cả các bảng trong database."""
        if self.engine:
            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            return list(metadata.tables.keys())
        return []
    
    def engineDispose(self):
        """Đóng kết nối engine thủ công."""
        return self.engine.dispose()
    
    def set_primary_key(self, targetTableName: str, pk_col: str = "id_date"):
        """Thiết lập Primary Key cho bảng (mặc định là cột id_date)."""
        logger.info(f"SET PRIMARY KEY FOR TABLE {targetTableName}")
        with self.engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE dbo.[{targetTableName}] ALTER COLUMN [{pk_col}] INTEGER NOT NULL;"))
            conn.execute(text(f"ALTER TABLE dbo.[{targetTableName}] ADD PRIMARY KEY ([{pk_col}]);"))
        logger.info(f"SET PRIMARY KEY FOR TABLE {targetTableName}: SUCCESSFUL!")

    def getData(self, tableName: str, columns: List[str] = None) -> pd.DataFrame:
        """Đọc toàn bộ dữ liệu từ một bảng."""
        with self.engine.connect() as conn:
            table = pd.read_sql_table(tableName, conn, columns=columns)
        return table
    
    def getData_slice(self, arg_choice, tableName, sort_by="id_date", is_exact=False) -> pd.DataFrame:
        """
        Lấy dữ liệu có điều kiện hoặc lấy Top N dòng.
        
        :param arg_choice: Số lượng dòng (int) hoặc điều kiện (str)
        :param is_exact: Nếu True, arg_choice là điều kiện WHERE. Nếu False, arg_choice là LIMIT.
        """
        if not tableName.isidentifier():
             logger.warning(f"Table name {tableName} may be unsafe.")

        if not is_exact:
            query = text(f"SELECT TOP(:limit) * FROM dbo.[{tableName}] ORDER BY [{sort_by}] DESC;")
            params = {"limit": arg_choice}
        else:
            # Lưu ý: Cần cẩn thận SQL Injection ở đây nếu arg_choice đến từ user input không tin cậy
            query = text(f"SELECT * FROM dbo.[{tableName}] WHERE [{sort_by}] {arg_choice};")
            params = {}

        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)

    def get_primary_key_column(self, tableName) -> Optional[str]:
        """Tìm tên cột Primary Key của bảng."""
        inspector = inspect(self.engine)
        pk_columns = inspector.get_pk_constraint(tableName, schema='dbo')
        if pk_columns and pk_columns.get('constrained_columns'):
            return pk_columns['constrained_columns'][0]
        # Fallback logic nếu không tìm thấy qua metadata
        for pk in ['id_date', 'id']:
            col_names = [col['name'] for col in inspector.get_columns(tableName, schema='dbo')]
            if pk in col_names:
                return pk
        return None

    def is_auto_increment(self, tableName, columnName) -> bool:
        """Kiểm tra cột có phải là tự động tăng (Identity) không."""
        inspector = inspect(self.engine)
        for col in inspector.get_columns(tableName, schema='dbo'):
            if col['name'] == columnName:
                return col.get('autoincrement', False) or col.get('identity', False)
        return False

    def check_row_duplicates(self, new_data, existing_data, compare_columns):
        """Loại bỏ các dòng trùng lặp dựa trên các cột so sánh."""
        if not compare_columns:
            return new_data
        valid_columns = [c for c in compare_columns if c in new_data.columns and c in existing_data.columns]
        if not valid_columns:
            return new_data
        
        new_sub = new_data[valid_columns].copy()
        old_sub = existing_data[valid_columns].copy()

        # Chuẩn hóa kiểu dữ liệu để so sánh chính xác
        for col in valid_columns:
            dtype_new = new_sub[col].dtype
            dtype_old = old_sub[col].dtype
            if np.issubdtype(dtype_new, np.datetime64) or np.issubdtype(dtype_old, np.datetime64) or \
               dtype_new == 'object' or dtype_old == 'object':
                try:
                    new_sub[col] = pd.to_datetime(new_sub[col], errors='coerce')
                    old_sub[col] = pd.to_datetime(old_sub[col], errors='coerce')
                except:
                    new_sub[col] = new_sub[col].astype(str)
                    old_sub[col] = old_sub[col].astype(str)

        try:
            merged = pd.merge(new_sub, old_sub, how='left', indicator=True)
            non_dupes_mask = merged['_merge'] == 'left_only'
            result = new_data.loc[non_dupes_mask]
            logger.info(f"Found {len(result)} non-duplicate rows out of {len(new_data)}")
            return result
        except Exception as e:
            logger.error(f"Error duplicate check: {e}")
            return new_data

    def _add_new_columns(self, targetTableName, list_diff, dataTable):
        """Tự động thêm cột mới vào bảng SQL nếu DataFrame có nhiều cột hơn."""
        list_diff_type = []
        for new_col in list_diff:
            if new_col in dataTable.columns:
                type_ = dataTable[new_col].dtypes
                if np.issubdtype(type_, np.floating):
                    list_diff_type.append("FLOAT")
                elif np.issubdtype(type_, np.integer):
                    list_diff_type.append("INT")
                elif np.issubdtype(type_, np.datetime64):
                     list_diff_type.append("DATE")
                else:
                    list_diff_type.append("NVARCHAR(MAX)") 
        for idx, col in enumerate(list_diff):
            self.addColumn(targetTableName, col, list_diff_type[idx])

    def insertData(self, dataTable, targetTableName, primaryKey=None, dtypes=None, unique_columns=None):
        """
        Insert dữ liệu thông minh:
        1. Tạo bảng nếu chưa có.
        2. Tự động thêm cột thiếu.
        3. Kiểm tra trùng lặp trước khi insert.
        """
        if not self.checkTableExist(targetTableName):
            self.createNewTable(dataTable, targetTableName)
            return

        if dataTable.empty:
            return

        # Fix lỗi Decimal object của Python làm crash pyodbc
        for col in dataTable.columns:
            if dataTable[col].dtype == 'object':
                try:
                    valid_vals = dataTable[col].dropna()
                    if not valid_vals.empty and isinstance(valid_vals.iloc[0], Decimal):
                        dataTable[col] = dataTable[col].astype(float)
                except Exception:
                    pass

        if dtypes is None:
            dtypes = {}
        string_cols = dataTable.select_dtypes(include=['object']).columns
        for col in string_cols:
            if col not in dtypes:
                dtypes[col] = types.NVARCHAR()

        logger.info(f"INSERTING DATA INTO {targetTableName}")

        if primaryKey is None:
            primaryKey = self.get_primary_key_column(targetTableName)
            
        skip_pk_check = False
        if primaryKey:
            if primaryKey not in dataTable.columns:
                skip_pk_check = True # PK tự tăng hoặc không có trong dữ liệu đầu vào
        else:
            skip_pk_check = True

        # Lấy mẫu dữ liệu cũ để so sánh cột và nội dung
        sort_col = primaryKey if (primaryKey and primaryKey in self.getData(targetTableName).columns) else None
        if sort_col:
            df_cache = self.getData_slice(2000, targetTableName, sort_by=sort_col, is_exact=False)
        else: 
            query = text(f"SELECT TOP 2000 * FROM dbo.[{targetTableName}]") 
            with self.engine.begin() as conn:
                df_cache = pd.read_sql(query, conn)

        existing_cols_lower = [c.lower() for c in df_cache.columns]
        new_cols = [c for c in dataTable.columns if c.lower() not in existing_cols_lower]
        if new_cols:
            self._add_new_columns(targetTableName, new_cols, dataTable)

        try:
            # Transaction Insert
            with self.engine.begin() as conn:
                if skip_pk_check:
                    compare_cols = unique_columns if unique_columns else [c for c in dataTable.columns if c != primaryKey]
                    data_to_insert = self.check_row_duplicates(dataTable, df_cache, compare_cols)
                    
                    if not data_to_insert.empty:
                        logger.info(f"INSERTING {len(data_to_insert)} ROWS")
                        data_to_insert.to_sql(targetTableName, conn, index=False, if_exists='append', dtype=dtypes, chunksize=20000)
                    else:
                        logger.info("NO NON-DUPLICATE ROWS TO INSERT")

                else:
                    existing_ids = set(df_cache[primaryKey].values)
                    new_ids = dataTable[~dataTable[primaryKey].isin(existing_ids)]
                    
                    if not new_ids.empty:
                        logger.info("INSERTING DATA (PK CHECK)")
                        new_ids = new_ids.drop_duplicates(subset=[primaryKey])
                        new_ids.sort_values(by=primaryKey, inplace=True)
                        new_ids.to_sql(targetTableName, conn, index=False, if_exists='append', dtype=dtypes, chunksize=20000)
                    else:
                        logger.info("FOUND OVERLAP OF PRIMARY KEY")

            # Logic Update (nếu cần thiết sau khi insert) có thể đặt ở đây
            # ...

        except Exception as e:
            logger.error(f"Error during insertion: {e}")
            logger.error(f"First row dtypes: {dataTable.dtypes}")
        
        logger.info(f"INSERT PROCESS FINISHED")

    def addColumn(self, targetTableName, columnName, data_type):
        query = text(f"ALTER TABLE [dbo].[{targetTableName}] ADD [{columnName}] {data_type} NULL")
        with self.engine.begin() as conn:
            conn.execute(query)

    def deleteData(self, table, condition):
        query = text(f"DELETE FROM [dbo].[{table}] WHERE id_date {condition};")
        with self.engine.begin() as conn:
            conn.execute(query)

    def createNewTable(self, dataTable, targetTableName, dtypes=None, force_to_float=True):
        if dtypes is None:
            dtypes = {}
            for col in dataTable.columns:
                if col == "id_date":
                    dtypes[col] = types.Integer()
                elif "date" in col.lower() or np.issubdtype(dataTable[col].dtype, np.datetime64):
                    dtypes[col] = types.Date()
                elif np.issubdtype(dataTable[col].dtype, np.number):
                    if force_to_float:
                         dtypes[col] = types.Float()
                else:
                    dtypes[col] = types.NVARCHAR() 
        
        with self.engine.begin() as conn:
            dataTable.to_sql(targetTableName, conn, index=False, if_exists='replace', dtype=dtypes, chunksize=20000)
            
        self.set_primary_key(targetTableName)
        logger.info(f"CREATE TABLE {targetTableName} SUCCESSFUL")

    def updateData(self, targetTableName, id_date_cond, update_column, update_value, primaryKey='id_date'):
        """Cập nhật một ô dữ liệu cụ thể."""
        assert isinstance(update_column, str)
        
        if hasattr(id_date_cond, 'item'):
            id_date_cond = id_date_cond.item()
        if hasattr(update_value, 'item'):
            update_value = update_value.item()

        params = {"val": update_value, "id": id_date_cond}
        if pd.isna(update_value):
            sql = f"UPDATE [dbo].[{targetTableName}] SET [{update_column}] = NULL WHERE {primaryKey} = :id"
        else:
            sql = f"UPDATE [dbo].[{targetTableName}] SET [{update_column}] = :val WHERE {primaryKey} = :id"
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql), params)
        except Exception as e:
            logger.error(f"Error executing update: {e}")
            raise

    def checkTableExist(self, targetTableName):
        self.tableInDB = self.getTableNameInDB()
        return targetTableName in self.tableInDB

@logger.catch
def dataInfo(yamlPath: str, databaseName: str = None):
    """Factory function để tạo instance dbJob từ file config YAML."""
    if yamlPath:
        with open(yamlPath, 'r') as stream:
            data = yaml.safe_load(stream)['db_info']
        server, uid, pwd = data['server'], data['username'], data['password']
        # Cho phép override database name nếu cần
        db_name = databaseName if databaseName else data.get('database')
    else:
        # Fallback to Environment Variables
        server = os.environ.get('DB_NAME')
        uid = os.environ.get('DB_USER')
        pwd = os.environ.get('DB_PASS')
        db_name = databaseName
        
    return dbJob(server, db_name, uid, pwd, '{ODBC Driver 17 for SQL Server}')

def clean_numeric_string(value):
    """Hàm helper để làm sạch chuỗi số (ví dụ: '1.5M' -> 1500000.0)."""
    if pd.isna(value): return np.nan
    s = str(value).strip().replace(',', '').replace('%', '').upper()
    if not s: return np.nan
    multipliers = {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}
    mult = 1.0
    if s and s[-1] in multipliers:
        mult = multipliers[s[-1]]
        s = s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return np.nan

@logger.catch
def check_and_update_table(df_new: pd.DataFrame, table_name: str, engine, key_columns: list, numeric_cols: list):
    """
    Hàm Sync dữ liệu mạnh mẽ:
    1. So sánh dữ liệu mới (df_new) với dữ liệu cũ trong DB.
    2. Insert các dòng chưa tồn tại.
    3. Update các dòng đã tồn tại nhưng có dữ liệu thay đổi.
    """
    def normalize_keys(df, keys):
        for key in keys:
            if key in df.columns:
                df[key] = df[key].astype(str).str.strip().str.lower()
        return df

    for col in numeric_cols:
        if col in df_new.columns:
            df_new[col] = df_new[col].apply(clean_numeric_string)
    
    df_new = normalize_keys(df_new, key_columns)
    df_new = df_new.drop_duplicates(subset=key_columns, keep='last').replace({np.nan: None})

    with engine.begin() as conn:
        try:
            df_old = pd.read_sql_table(table_name, con=conn)
            if 'id' in df_old.columns: df_old.drop('id', axis=1, inplace=True)
            for col in numeric_cols:
                if col in df_old.columns:
                    df_old[col] = df_old[col].apply(clean_numeric_string)
            df_old = normalize_keys(df_old, key_columns)
            df_old = df_old.drop_duplicates(subset=key_columns, keep='last').replace({np.nan: None})
            value_columns = [c for c in df_new.columns if c not in key_columns and c in df_old.columns]
        except Exception as e:
            logger.warning(f"Could not read table {table_name}: {e}")
            df_old = pd.DataFrame(columns=df_new.columns)
            value_columns = [c for c in df_new.columns if c not in key_columns]

        merged = pd.merge(df_new, df_old, on=key_columns, how='left', suffixes=('', '_old'), indicator=True)

        # INSERT
        to_insert = merged[merged['_merge'] == 'left_only'][df_new.columns]
        if not to_insert.empty:
            logger.info(f"INSERTING {len(to_insert)} NEW ROWS")
            to_insert.to_sql(table_name, con=conn, if_exists='append', index=False)
        else:
            logger.info("NO NEW DATA TO INSERT")

        # UPDATE
        to_update = merged[merged['_merge'] == 'both']
        rows_to_update = []
        if not to_update.empty:
            for _, row in to_update.iterrows():
                if any(row[col] != row[f"{col}_old"] and not (pd.isna(row[col]) and pd.isna(row[f"{col}_old"])) 
                       for col in value_columns):
                    update_row = {k: row[k] for k in key_columns}
                    update_row.update({v: row[v] for v in value_columns})
                    rows_to_update.append(update_row)

        if rows_to_update:
            logger.info(f"UPDATING {len(rows_to_update)} ROWS")
            set_clause = ", ".join([f"[{col}] = :{col}" for col in value_columns])
            where_clause = " AND ".join([f"[{key}] = :{key}" for key in key_columns])
            stmt = text(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}")
            conn.execute(stmt, rows_to_update)
        else:
            logger.info("NO EXISTING ROWS NEED UPDATING")
    
    logger.success(f"SYNC COMPLETE FOR {table_name}")