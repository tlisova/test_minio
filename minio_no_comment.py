import numpy as np
from minio import Minio
import pandas as pd
import re


def main():
    pd.set_option('display.max_columns', None)

    client = Minio("127.0.0.1:9000", access_key="tlisova", secret_key="tlisova98", secure=False, region="ru")

    client.fget_object("test-bucket-201170", "201170.csv", "./test_csv.csv")

    with open("test_csv.csv", encoding="cp1251") as file:
        csvframe = pd.read_csv(file, delimiter=';', header=None)
        
        csvframe[0] = csvframe[0].map(lambda x: str(x)[-10:])
        
        s3 = pd.DataFrame(csvframe[3].str.split(' ', 1, expand=True).values.tolist(), columns=['ooo', 'name'])

        csvframe[8] = pd.to_numeric(csvframe[8].apply(lambda x: re.sub(',', '.', str(x).replace(' ', ''))))

        s9 = pd.DataFrame(csvframe[9].str.split('-', expand=True).values.tolist(), columns=['g1', 'g2'])

        res = pd.concat([csvframe, s3, s9], axis=1)

        res = res[[0, 2, 'ooo', 'name', 4, 5, 6, 7, 8, 'g1', 'g2']]
        print(res)

        res.to_csv('new_201170.csv', sep=';', index=False)

        client.fput_object("test-bucket-201170", "new_201170.csv", "new_201170.csv")


if __name__ == '__main__':
    main()

