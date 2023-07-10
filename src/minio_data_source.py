from athena.federation.athena_data_source import AthenaDataSource

import pyarrow as pa
from pyarrow import  parquet
from typing import Mapping, Dict, List, Any   
 

class MinioDataSource(AthenaDataSource):
    """
    An example Athena Data Source that connects Athena to Minio
    """
    def __init__(self,minio_client, minio_pya_client,buckets,tables_path,bucket_prefix):
        super().__init__() 
        self.minio_client =  minio_client
        self.minio_pya_client  = minio_pya_client
        self.buckets = buckets
        self.tables_path= tables_path  
        self.bucket_prefix= bucket_prefix    
    
    def databases(self) -> List[str]:  
        buckets_list=[]
        for bucket in self.buckets:
            if  bucket.name.startswith(self.bucket_prefix):
                buckets_list.append(bucket.name)
        return buckets_list
    
    def tables(self, database_name: str) -> List[str]: 
        
        objects = get_minio_bucket_tables (self.minio_client,database_name,self.tables_path)
        tables_list=[]
        for obj in objects: 
            if obj.object_name != self.tables_path:
                tables_list.append(obj.object_name[:-1].partition(self.tables_path)[2] ) 
        return tables_list 
      
    def columns(self, database_name: str, table_name: str) -> List[str]:
        pf = f_get_table_files (database_name, table_name, self.minio_client, self.minio_pya_client,self.tables_path)
        columns_list = []
        for item in pf.schema:
            columns_list.append(item.name)
        return columns_list
    
    def schema(self, database_name: str, table_name: str) -> pa.Schema:
        return super().schema(database_name, table_name) 
    
    def splits(self, database_name: str, table_name: str) -> List[Dict]:
        return [
            {
                "name": "split1",
                "action": "normal"
            } 
        ]

    def records(self, database: str, table: str, split: Mapping[str,str]) -> Dict[str,List[Any]]:
        """
        Generate example records
        """ 
        print("print calling records")
        pf = f_get_table_files (database, table, self.minio_client, self.minio_pya_client,self.tables_path)
        return pf.to_pydict()
    

def get_minio_bucket_tables (minio_client,database_name,tables_path):
    """
    Function that returns a list of tables (folders) in a minio bucket
    Inputs:
        - database_name: Minio Bucket Name 
        - minio_client: Minio client 
        - tables_path: path to check when retrieving tables
    Output:
        - a list of table names (folders inside the bucket matching the provided path)
    """
    return minio_client.list_objects(bucket_name= database_name,prefix=tables_path)


def f_get_table_files (database_name, table_name, minio_client, minio_pya_client,prefix): 
    """
    Function that reads records from a Minio folder and returns a pyarrow table
    Inputs:
        - database_name: Minio Bucket Name
        - table_name: Minio folder containing parquet files inside database_name bucket
        - minio_client: Minio client
        - minio_pya_client: Minio pyarrow client
        - prefix: path to table_name
    Output:
        - pyarrow table containing table_name data
    """ 
    objects=get_minio_bucket_tables(minio_client,database_name, prefix + table_name+ '/') 
    table_files_list = []
    for obj in objects:
        if obj.object_name != prefix+ table_name+'/' and obj.object_name.endswith('.parquet'):
            table_files_list.append(database_name +'/'+obj.object_name)
    pf = parquet.read_table(table_files_list,filesystem= minio_pya_client)
    return pf