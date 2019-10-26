# PythonからBigQueryを読み書きする

## 環境
```
macOS Mojava ver 10.14.6
pyenv, pipenv, python ver. 3.6.8
```

## ローカル設定
忘れてしまって申し訳ないんですが、多分、gcloudの設定をPCにしておく必要性があった気がする...
```
$ curl https://sdk.cloud.google.com | bash
$ pipenv install gcloud
```

今回使うライブラリをインストール
```
$ pipenv install google-cloud-bigquery, pyarrow, pandas
```

## GCP設定
### サービスアカウントの設定
IAMと管理のサービスアカウントから、サービスアカウントを作成する

![スクリーンショット 2019-10-26 12 39 27](https://user-images.githubusercontent.com/17031124/67613886-e4d53a80-f7ed-11e9-92b1-5ba86d462786.png)


### クレデンシャル情報の取得
Cloud APIを使用する(ローカルからGCPを操作する)場合、サービスを使うための認証情報が必要になるので取得する

[Google Cloud Document 認証](https://cloud.google.com/docs/authentication/getting-started)

![image](https://user-images.githubusercontent.com/17031124/67613762-29f86d00-f7ec-11e9-8eda-7f36b7088e75.png)

1. ServiceAccountを入力し、keyタイプをJSONに選択する。
2. Createボタンを押すとダウンロード画面に行くため、下記フォルダ構成のようにjsonファイルを配置する

[Create Service account key](https://console.cloud.google.com/apis/credentials/serviceaccountkey)

![スクリーンショット 2019-10-26 12 28 07](https://user-images.githubusercontent.com/17031124/67613814-abe89600-f7ec-11e9-8241-12cfa979ae76.png)


## フォルダ構成
```
├── main.py                          <- 実行するファイル
├── utils
    ├── operation_bigquery           <- BigQuery操作に関するクラス
    └── credential-344323q5e32.json  <- クレデンシャル情報
```

## ソースコード

operation_bigquery.py
```python=
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
    def write_bq(self, dataframe: pandas.core.frame.DataFrame, dataset: str, table:str) -> None:
        config = self.client.dataset(dataset).table(table)
        self.client.load_table_from_dataframe(dataframe, config).result()
    '''

```

credential-344323q5e32.json
```jsonld=
{
  "type": "service_account",
  "project_id": "project-291031",
  "private_key_id": "464564c7f86786afsa453345dsf234vr32",
  "private_key": "-----BEGIN PRIVATE KEY-----\ndD\n-----END PRIVATE KEY-----\n",
  "client_email": "my-email-address@project-291031.iam.gserviceaccount.com",
  "client_id": "543423423542344334",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/d453/my-email-address@project-291031.iam.gserviceaccount.com"
}

```

## BigQueryからPythonのpandas.DataFrameへ読み込み
main.py
```python=
from utils.operation_bigquery import Bigquery_to_Pandas

download_table_path = {
    "project": "project-291031",
    "dataset": "datawarehouse",
    "table": "bigquery_test_table",
    "credential_path": "utils/credential-344323q5e32.json",
    "if_exists": "replace"
}

dataframe = Bigquery_to_Pandas(download_table_path).read_bq()
```

## Pythonのpandas.DataFrameからBigQueryへの書き込み
main.py
```python=
from utils.operation_bigquery import Bigquery_to_Pandas

upload_table_path = {
    "project": "project-291031",
    "dataset": "datamart",
    "table": "bigquery_test_describe_table",
    "credential_path": "utils/credential-344323q5e32.json",
    "if_exists": "replace"
}

Bigquery_to_Pandas(upload_table_path).write_bq(dataframe)
```
