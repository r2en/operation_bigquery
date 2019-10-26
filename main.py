import numpy
import pandas
from utils.operation_bigquery import Bigquery_to_Pandas

def main():

    # 書き込み
    upload_table_path = {
        "project": "project-291031",
        "dataset": "datamart",
        "table": "bigquery_test_describe_table",
        "credential_path": "utils/credential-344323q5e32.json",
        "if_exists": "replace"
    }

    dataframe = pandas.DataFrame(numpy.random.randn(6,4), columns=['A', 'B', 'C', 'D'])
    Bigquery_to_Pandas(upload_table_path).write_bq(dataframe)


    # 読み込み
    download_table_path = {
        "project": "project-291031",
        "dataset": "datawarehouse",
        "table": "bigquery_test_table",
        "credential_path": "utils/credential-344323q5e32.json",
        "if_exists": "replace"
    }

    dataframe = Bigquery_to_Pandas(download_table_path).read_bq()

if __name__ == '__main__':
    main()