import pytest
from src.minio_data_source import MinioDataSource
from minio.datatypes import Bucket, Object
from datetime import date 
import pyarrow as pa


class TestMinioDataSource():
    
    dt = date(2023, 1, 1)
    obj = MinioDataSource(minio_client = '', minio_pya_client = '',buckets = [Bucket('bucket1', dt),Bucket('testbucket', dt),Bucket('buck', dt),Bucket('dummy', dt)],tables_path = '',bucket_prefix ='')
 
    #region testing databases function

    @pytest.mark.parametrize(
	"input_val,expected_val", [ ("", ['bucket1', 'testbucket', 'buck' ,'dummy']), #all
                                ("buck",['bucket1',  'buck' ]), #buck as prefix
                                ('a',[]) #no result
                              ]
    )

    def test_get_minio_buckets_all(self,input_val,expected_val):   
        self.obj.bucket_prefix   =input_val
        result = self.obj.databases()
        assert result == expected_val

    #endregion
    
    #region testing tables function

    def create_mock_tables_from_path(self, bucket_name,tables_path):
        tables_list = []
        tables_list.append(Object(bucket_name,tables_path))
        tables_list.append(Object(bucket_name,tables_path+ 'table1/'))
        tables_list.append(Object(bucket_name,tables_path+ 'table2/'))
        tables_list.append(Object(bucket_name,tables_path+ 'table3/'))
        return tables_list

    def test_get_minio_tables (self, mocker):
        tables_list = self.create_mock_tables_from_path('bucket','f1/f2/f3/')
        print(tables_list)
        mocker.patch( "src.minio_data_source.get_minio_bucket_tables", return_value = tables_list) 
        self.obj.tables_path = 'f1/f2/f3/'
        result = self.obj.tables('bucket') 
        assert result == ['table1','table2','table3']

    #endregion

    #region testing schema and records

    def create_mock_table_entries(self):
        id = pa.array([1,2,3,4])
        value = pa.array(['value1','value2','value3','value4'])
        return pa.Table.from_arrays([id,value], names =['id','value'])

    def test_get_table_schema (self, mocker):
        pf = self.create_mock_table_entries() 
        mocker.patch( "src.minio_data_source.f_get_table_files", return_value = pf) 
        result = self.obj.columns('bucket', 'table')
        assert result == ['id','value']

    def test_get_table_records (self, mocker):
        pf = self.create_mock_table_entries() 
        mocker.patch( "src.minio_data_source.f_get_table_files", return_value = pf) 
        result = self.obj.records('bucket', 'table', 'split1')
        assert result == pf.to_pydict()
         
    #endregion