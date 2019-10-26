'''
BigQuery操作に関するクラス
'''

import os
import pandas
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from typing import List, Set, Dict, Tuple, TypeVar, Callable

class Bigquery_to_Pandas():
    '''
    BigQuery操作に関するクラス
    BigQueryのデータをダウンロードしてDataFrameに変換する
    DataFrameのデータをアップロードしてBigQueryのデータに変換する
    '''
    def __init__(self, parameter: Dict[str, str]) -> None:
        self.project = parameter['project']
        self.dataset = parameter['dataset']
        self.table = parameter['table']
        self.if_exists = parameter['if_exists']
        path = parameter['credential_path']
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str((Path(Path.cwd()).parent)/parameter["credential_path"])
        self.credentials = str((Path(Path.cwd()).parent)/parameter["credential_path"])

        self.client = bigquery.Client.from_service_account_json(self.credentials)
    
    def read_bq(self) -> pandas.core.frame.DataFrame:
        query = f'SELECT * FROM `{self.project}.{self.dataset}.{self.table}`'
        dataframe = self.client.query(query, project=self.project).to_dataframe()
        return dataframe
    
    def write_bq(self, dataframe: pandas.core.frame.DataFrame) -> None:
        dataframe.to_gbq(f'{self.dataset}.{self.table}', project_id=self.project, if_exists=self.if_exists)
    
    '''
    ・ライブラリ側にバグが有るためこちらは使用しない
    https://github.com/googleapis/google-cloud-python/issues/7370
    def write_bq(self, dataframe: pandas.DataFrame, dataset: str, table:str) -> None:
        config = self.client.dataset(dataset).table(table)
        self.client.load_table_from_dataframe(dataframe, config).result()
    '''

