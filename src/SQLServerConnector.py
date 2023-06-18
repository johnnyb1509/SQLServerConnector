# This Python file contains functions for working with SQL Server
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import NVARCHAR
from sqlalchemy.engine import URL
from loguru import logger
#========================================================
# NOTICE only use SQLAlchemy version < 2.0
#========================================================

# CONNECT TO DB
class dbJob():
    # TODO: dev DELETE and REPLACE function for this class
    def __init__(self, server, database, username, password, driver):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.engine, self.tableInDB = self.sqlServerConnect()
        
    def sqlServerConnect(self):
        """Function for creating engine to connect to the SQL
            AND get all table name which included in the given database
            Using SQLAlchemy
        Args:
            database (str): database target name
            username (str): user account name
            password (str): password
            driver (str): driver name 

        Returns:
            engine (sqlalchemy object): engine for connecting
        """
        # conn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        connection_string = f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}"
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url, pool_pre_ping=True, pool_size = -1)

        list_tableInDB = inspect(engine).get_table_names()
        return engine, list_tableInDB
    
    def engineDispose(self):
        return self.engine.dispose()
    
    def set_primary_key(self, targetTableName):
        '''This function sets a primary key for a specified table in a SQL database.
        
        Parameters
        ----------
        targetTableName
            The name of the table for which the primary key needs to be set.
        
        Returns
        -------
            nothing (i.e., None).
        
        '''
        logger.info(f"SET PRIMARY KEY FOR TABLE {targetTableName}")
        change_notnull = f"ALTER TABLE dbo.{targetTableName} ALTER COLUMN id_date INTEGER NOT NULL;"
        add_primaryKey = f"ALTER TABLE dbo.{targetTableName} ADD PRIMARY KEY (id_date);"
        with self.engine.connect() as conn:
            conn.execute(change_notnull)
            conn.execute(add_primaryKey)
            logger.info(f"SET PRIMARY KEY FOR TABLE {targetTableName}: SUCCESSFUL!")
            conn.close()
        return

    def getData(self, tableName, columns=None):
        """Read table from SQL Server at self.database

        Args:
            tableName (str): Name of the table that wanted to get
            connection (SQLAlchemy object): self.engine.connect()

        Returns:
            pd.DataFrame
        """
        with self.engine.connect() as conn:
            table = pd.read_sql_table(tableName, conn, columns=columns)
            conn.close()
        return table
    
    def getData_slice(self, arg_choice, tableName, sort_by = "id_date", is_exact = False):
        """Get little data just for checking columns name and <n_row> latest data
            NOTICE: JUST GET TABLE WITH id_date AS PRIMARY KEY
        

        Args:
            arg_choice (string): If is_exact == False -> Number of rows want to get 
                                        (e.g 10 rows or 100 rows)
                                Elif is_exact == True -> id_date number of row want to get 
                                            (e.g row with id_date " > <arg_choice>")
            tableName (str): Name of the table
        """
        short_table = None
        if is_exact == False:
            getData_query = f"SELECT TOP({arg_choice}) * FROM {self.database}.dbo.{tableName} ORDER BY {sort_by} DESC;"
            # logger.info(f"DEBUGGING ------- {getData_query}")
            short_table = pd.read_sql(getData_query, self.engine)
        elif is_exact == True:
            getExact_query = f"SELECT * FROM {self.database}.dbo.{tableName} WHERE {sort_by} {arg_choice};"
            short_table = pd.read_sql(getExact_query, self.engine)
        return short_table
    
    def getDataByDate(self, tableName, dateColumn, fromDate: str, toDate: str):
        getExact_query = f"SELECT  * \
                    FROM  {self.database}.dbo.{tableName} \
                    WHERE {dateColumn} >= {fromDate} and {dateColumn} <= '{toDate}'"
        dataGet = pd.read_sql(getExact_query, self.engine)
        return dataGet

    def insertData(self, dataTable, targetTableName, dtypes = None):
        """Insert data from pd.DataFrame to table on SQL Server using SQLALchemy

        Args:
            dataTable (pd.DataFrame): DateFrame that includes row (rows) to insert.
                                        The order of columns is not important
            targetTableName (str): name of the table that included in self.database
        """
        check_table = self.checkTableExist(targetTableName) # Check if targetTableName exist
        
        if check_table == True and len(dataTable) != 0:
            logger.info("---------------------------------------")
            logger.info(f"INSERTING DATA INTO {targetTableName}")
            logger.info("CHECKING COLUMNS DIFFERENCE")
            
            # Pull before push to compare columns
            df_cache = self.getData_slice(1000, targetTableName, is_exact=False)
            list_old_columns = list(df_cache.columns)
            list_new_columns = list(dataTable.columns)
            list_diff = [item for item in list_new_columns if item not in list_old_columns]# list(set(list_new_columns) - set(list_old_columns)) # component in list_new but not
                                                                            # in list_old
            list_diff_type = list()

            if len(list_diff) != 0: # if there is any new columns, create T-SQL type for columns
                logger.info("APPEAR COLUMNS DIFFERENCE, ADDING COLUMN(s)")
                for new_col in list_diff:
                    type_ = dataTable[new_col].dtypes
                    if type_ == "float64":
                        list_diff_type.append("FLOAT")
                    elif type_ == "int64":
                        list_diff_type.append("INT")
                    elif type_ == "O":
                        list_diff_type.append("DATE")
                    else:
                        list_diff_type.append("VARCHAR")
                        
                for new_col_idx in range(len(list_diff)):
                    logger.info(f"--------- ADDING COLUMN(s) {list_diff[new_col_idx]}")
                    self.addColumn(targetTableName, list_diff[new_col_idx], list_diff_type[new_col_idx])

            listIdNewDataTable = list(dataTable['id_date'].values)
            listIdOldDataTable = list(df_cache['id_date'].values)
            
            # check violation of primary key
            listNew_idDate = [item for item in listIdNewDataTable if item not in listIdOldDataTable]
            if len(listNew_idDate) != 0:
                logger.info("NO OVERLAP OF PRIMARY KEY, INSERTING DATA INTO TABLE")
                listNew_idDate.sort()
                # logger.info(f"DEBUGGING --- {listNew_idDate}")
                newidDate_table = dataTable[dataTable['id_date'].isin(listNew_idDate)]
                newidDate_table.to_sql(targetTableName, self.engine.connect(), index=False, if_exists = 'append',
                                dtype = dtypes, chunksize=20000)
            elif len(listNew_idDate) == 0:
                logger.info("FOUND OVERLAP OF PRIMARY KEY")
            
            # update if has new columns
            is_diffData, self.data_checkDict = self.compareDataRow(dataTable, targetTableName)
            if len(list_diff) != 0: 
                logger.info("APPEAR NEW COLUMN(S), INSERTING NEW COLUMN(S)")
                for new_col in list_diff:
                    self.updateData(targetTableName, dataTable['id_date'].values[0], 
                                    new_col, dataTable[new_col].values[0])
            elif len(list_diff) == 0:
                logger.info(f"NO NEW COLUMN TO PUSH")
                
            # update if has different data
            if is_diffData == True:
                logger.info("FOUND DIFFERENT IN DATA, UPDATING COLUMNS DATA")
                for column, value in self.data_checkDict.items():
                    logger.info(f"------ UPDATING COLUMNS {column}")
                    self.updateData(targetTableName, dataTable['id_date'].values[0], 
                                    column, value)
            else:
                logger.info(f"ALL DATA VALUE IS THE SAME FOR id_date {dataTable['id_date'].values[0]}")
                   
            logger.info(f"INSERT DATA INTO {targetTableName} FINISH")
            logger.info("---------------------------------------")
            
        elif check_table == False: # if not > creating new table
            logger.info(f"TABLE {targetTableName} DOES NOT EXIST IN DATABASE {self.database}")
            logger.info(f"CREATING NEW TABLE {targetTableName}")
            self.createNewTable(dataTable, targetTableName)
        return
    
    
    def insertData_VBMADanhMuc(self, dataTable, targetTableName, dtypes = None, fromsource = "VBMA"):
        """Insert data from pd.DataFrame to table 'debt_danhmuc_rawData' - 'debt_data'
                on SQL Server using SQLALchemy

        Args:
            dataTable (pd.DataFrame): DateFrame that includes row (rows) to insert.
                                        The order of columns is not important
            targetTableName (str): name of the table that included in self.database
            fromsource (str): for identical using, mixing with HNX data pushing task
        """
        check_table = self.checkTableExist(targetTableName) # Check if targetTableName exist
        if check_table == True:
            logger.info("---------------------------------------")
            logger.info(f"INSERT DATA INTO {targetTableName}")
            logger.info("CHECKING COLUMNS DIFFERENCE")
            
            # Pull before push to compare columns
            df_cache = self.getData(targetTableName)
            list_old_columns = list(df_cache.columns)
            list_new_columns = list(dataTable.columns)
            list_diff = [item for item in list_new_columns if item not in list_old_columns]# list(set(list_new_columns) - set(list_old_columns)) # component in list_new but not
                                                                            # in list_old
            list_diff_type = list()

            if len(list_diff) != 0: # if there is any new columns, create T-SQL type for columns
                logger.info("APPEAR COLUMNS DIFFERENCE, ADDING COLUMNS")
                for new_col in list_diff:
                    type_ = dataTable[new_col].dtypes
                    if type_ == "float64":
                        list_diff_type.append("FLOAT")
                    elif type_ == "int64":
                        list_diff_type.append("INT")
                    elif type_ == "O":
                        list_diff_type.append("DATE")
                    else:
                        list_diff_type.append("VARCHAR")
                        
                for new_col_idx in range(len(list_diff)):
                    logger.info(f"--------- ADDING COLUMN(s) {list_diff[new_col_idx]}")
                    self.addColumn(targetTableName, list_diff[new_col_idx], list_diff_type[new_col_idx])
            
            if fromsource == "VBMA":
                check_unique_db = list(df_cache['ngay_TCPH'].unique())
                check_unique_newData = list(dataTable['ngay_TCPH'].unique())
                
                delete_row = list()
                logger.info("CHECKING UNIQUE ngay_TCPH")
                for new in check_unique_newData:
                    if new in check_unique_db:
                        delete_row.append(new)
                for del_row in delete_row:
                    dataTable = dataTable.drop(dataTable.index[dataTable['ngay_TCPH'] == del_row])
                
            logger.info(f"DATA PUSH \n {dataTable}")
            if len(dataTable) > 0:
                logger.info(f"THERE ARE {len(dataTable)} NEW ROW(S) TO PUSH")
                dataTable.to_sql(targetTableName, self.engine.connect(), index=False, if_exists = 'append',
                                        dtype = dtypes, chunksize=20000)
                logger.info(f"INSERT DATA INTO {targetTableName} SUCCESSFUL")
                logger.info("---------------------------------------")
            else:
                logger.info("NO NEW DATA TO PUSH")
            
        elif check_table == False: # if not > creating new table
            logger.info(f"TABLE {targetTableName} DOES NOT EXIST IN DATABASE {self.database}")
            logger.info(f"CREATING NEW TABLE {targetTableName}")
            self.createNewTable(dataTable, targetTableName)
        return
    
    def addColumn(self, targetTableName, columnName, data_type):
        """Add new columns and define its datatype 

        Args:
            targetTableName (str): Name of table
            columnName (_type_): _description_
            data_type (_type_): _description_
        """
        query = f"ALTER TABLE [dbo].[{targetTableName}] ADD [{columnName}] {data_type} NULL"
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.close()
        return
    
    def deleteData(self, table, condition):
        """Delete row with the defined condition on 'id_date' column

        Args:
            table (string): table that need to be affected 
            condition (string): e.g "= 20130405"
        """
        logger.info(f"DELETE ROW id_date = {condition} IN TABLE {table}")
        query = f"DELETE FROM [dbo].[{table}] WHERE id_date {condition};"
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.close()
        return
    
    def createNewTable(self, dataTable, targetTableName, dtypes = None, force_to_float = True):
        """Create new table on SQL Server then add Primary Key to the targetTableName

        Args:
            dataTable (pd.DataFrame): new table that want to push
            targetTableName (String): Name of the table
            dtypes (dict, optional): Specify DataType of each columns. Defaults to None.
        """
        logger.info(f"CREATE/REPLACE TABLE DATA: {targetTableName}")
        # Push table to sql
        if dtypes == None: # Auto create data type using SQLAlchemy datatype
            import sqlalchemy
            dtypes = dict()
            for col in dataTable.columns:
                if col == "id_date":
                    dtypes[col] = sqlalchemy.types.Integer()
                elif col == "date":
                    dtypes[col] = sqlalchemy.types.DATE()
                else:
                    if force_to_float == True:
                        dtypes[col] = sqlalchemy.types.Float()
        if dtypes == None and force_to_float == False:
            dataTable.to_sql(targetTableName, self.engine.connect(), index = False,
                            if_exists = 'replace', chunksize=20000)
        else:
            dataTable.to_sql(targetTableName, self.engine.connect(), index = False,
                                if_exists = 'replace', dtype = dtypes, chunksize=20000)

        self.set_primary_key(targetTableName)
            
        logger.info(f"CREATE/REPLACE TABLE DATA {targetTableName} SUCCESSFUL")
        logger.info("---------------------------------------")
        return
    
    def replaceTable(self, dataTable, targetTableName, nvarcharCols = None):
        """replace entire table on SQL Server

        Args:
            dataTable (pd.DataFrame): dataframe used to replace the exist data table on SQL Server
            targetTableName (str): target table for replacing
            nvarcharCols (list, optional): list of columns needed to be use NVARCHAR datatype. Defaults to None.
        """
        logger.info(f"REPLACE TABLE DATA: {targetTableName}")
        if nvarcharCols is not None:
            dataTable.to_sql(targetTableName, 
                             self.engine.connect(), 
                             index = False,
                            if_exists = 'replace', 
                            chunksize=20000, 
                            dtype = {
                                colName: NVARCHAR for colName in nvarcharCols
                            })
        else:
            dataTable.to_sql(targetTableName, self.engine.connect(), index = False,
                                if_exists = 'replace', chunksize=20000)
        logger.info(f"REPLACE TABLE DATA {targetTableName} SUCCESSFUL")
        logger.info("---------------------------------------")
        return
    
    def updateData(self, targetTableName, id_date_cond, update_column, update_value):
        """Update data for specific row (depends on id_date value) for ex

        Args:
            targetTableName (str): name of the table in self.database.
            id_date_cond (int): value of id_date column (PRIMARY KEY,)
            update_column (str): name of the columns that want to update
            update_value (float): value to be updated
        """
        logger.info("UPDATING DATA")
        assert type(update_column) is str
        logger.info(f"NEW VALUE AT {update_column} -- {update_value} -- TYPE {type(update_value)}")
        
        if (update_value is None or pd.isnull(update_value) or update_value  == np.nan):
            logger.info("CASE 1 -- NAN")
            logger.info("CHANGE NAN VALUE TO COMPATIBLE VALUE")
            update_query = f"UPDATE [dbo].[{targetTableName}] \
                            SET [{update_column}] = NULL \
                            WHERE id_date = {id_date_cond}"
                            
        elif (type(update_value) == int or type(update_value) == float):
            logger.info("CASE 2 -- INT OR FLOAT")
            update_query = f"UPDATE [dbo].[{targetTableName}] \
                            SET [{update_column}] = {update_value} \
                            WHERE id_date = {id_date_cond}"
        else:
            logger.info("CASE 2 -- STRING")
            update_query = f"UPDATE [dbo].[{targetTableName}] \
                            SET [{update_column}] = '{update_value}' \
                            WHERE id_date = {id_date_cond}"
        
        # if update_value is None or np.isnan(update_value): # cannot set the value = nan directly, SQL Server raise Error
                            
        with self.engine.connect() as conn:
            conn.execute(update_query)
            conn.close()
        return
    
    def compareDataRow(self, newTable, targetTableName):
        """This function tend to check data each column for only one specific row.
            USE CASE: check if there are any duplicate value between old and new data

        Args:
            newTable (pd.DataFrame): table of new data
            targetTableName (str): name of the table on database

        Returns:
            is_diff (bool): True if there are any different value, else False
            data_cache (dict): dictionary of data.
        """
        logger.info("COMPARING OLD DATA WITH NEW DATA")
        oldTable = self.getData_slice(f"= {newTable['id_date'].values[0]}", targetTableName, is_exact=True)
        oldTable = oldTable.drop(['id_date', 'date'], axis = 1)

        return self.compareProcess(oldTable, newTable)
    
    def compareProcess(self, oldTable, newTable, is_check_date = True, **kwargs):
        """Compare 2 DataFrame (only contains 1 row)

        Args:
            oldTable (DataFrame): Old data
            newTable (DataFrame): New data
            is_check_date (bool, optional): whether need to check date and id_date or not. Defaults to True.
            **kwargs: Just contain only key 'date_column'. Value is list
            
        Returns:
            (bool, dict)
        """
        data_cache = oldTable.to_dict('records')[0]
        isDataEqual_checkList = list()
        if is_check_date == False:
            oldTable = oldTable[oldTable.columns.difference(kwargs['date_column'])]
            newTable = newTable[newTable.columns.difference(kwargs['date_column'])]
        if len(oldTable) != 0:
            for column in list(oldTable.columns):
                if column in list(newTable.columns):
                    if oldTable[column].values[0] != newTable[column].values[0]:
                        assert type(column) == str
                        data_cache[column] = newTable[column].values[0]
                        isDataEqual_checkList.append(1)
                    else:
                        isDataEqual_checkList.append(0)
            # logger.info(f"DEBUGGING ---- CHECKLIST DATA EQUAL \n {isDataEqual_checkList}")
        is_diff = False
        if 1 in isDataEqual_checkList:
            is_diff = True
        # logger.info(f"DEBUGGING ---- IS_DIFF \n {is_diff}")
        return is_diff, data_cache
    
    def dropTable(self, tableName):
        query = f"DROP TABLE {tableName} ;"
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.close()
        return

    def checkPrimaryKeyExist(self, value, oldTable):
        is_exist = False
        if value in oldTable["id_date"].values:
            is_exist = True
        return is_exist
    
    def checkTableExist(self, targetTableName):
        is_tableExist = False
        if targetTableName in self.tableInDB:
            is_tableExist = True
        return is_tableExist
    
    def _cleanColAllNan(self, tableName):
        logger.info(f'----- CLEARING NAN COLUMNS TABLE {tableName} AND REPLACE')
        tableCache = self.getData(tableName)
        tableCache = tableCache.dropna(axis=1, how='all')
        self.replaceTable(tableCache, tableName)
        logger.info(f'----- CLEARING NAN COLUMNS TABLE {tableName} AND REPLACE: DONE')
        return
    
    def deleteColumnAllNan(self, tableName = None, isMassClean = False):
        """Function to remove columns which contains all NaN
            2023-11-01: Only use for database XNK_ChiTiet_data
        """
        if tableName is None and isMassClean == True:
            logger.info(f"MASS CLEAN MODE BEGIN, CLEAN ALL SPARE COLUMNS IN EVERY DATA TABLE OF THIS DATABASE {self.database}")
            for table_name in self.tableInDB:
                self._cleanColAllNan(table_name)
        elif tableName is not None and isMassClean == False:
            logger.info("CLEAN NAN COLUMNS FOR {tableName} ONLY")
            self._cleanColAllNan(tableName)
        logger.info("CLEARING SPARE COLUMN: DONE")
        return
    
    def deleteRowAllNan(self, tableName, skipColumns = None):
        """Method to pop row in DataFrame if all columns is NaN

        Args:
            df (pd.DataFrame): input DataFrame
            skipColumns (list, optional): list of column(s). Defaults to None.

        Returns:
            pd.DataFrame: Updated DataFrame 
        """
        logger.info(f"DELETE ROW CONTAIN ALL NAN IN TABLE {tableName}")
        update_df = self.getData(tableName)
        df_skipCols = self.getData(tableName)
        
        if skipColumns is not None:
            df_skipCols = df_skipCols.copy()[df_skipCols.columns.difference(skipColumns)]
            
        for idx, row in df_skipCols.iterrows():
            try:   
                if df_skipCols.iloc[idx].isnull().sum()/len(df_skipCols.iloc[idx]) == 1.0:
                    update_df = update_df.drop(idx)
            except:
                if df_skipCols.loc[idx].isnull().sum()/len(df_skipCols.loc[idx]) == 1.0:
                    update_df = update_df.drop(idx)
        self.replaceTable(update_df, tableName)
        logger.info(f"DELETE ROW CONTAIN ALL NAN IN TABLE {tableName}: DONE")
        return 
    
    
# # TESTING FUNCTION
# if __name__ == '__main__':
#     from pandas.io import sql
#     import numpy as np
#     import pandas as pd
#     import os
#     import socket
#     server = 
#     database = 
#     username = 
#     password = 
#     driver = '{ODBC Driver 17 for SQL Server}' # at NS_HomeServer
    
    
#     # INSERT DATA
#     module = dbJob(server, database, username, password, driver) # define module 
#     # print(module.getData_slice(10, "gdphh_rawData"))
#     print(module.tableInDB)
