import numpy as np
from minio import Minio
import pandas as pd
import re


def main():

    # чтобы отображались все колонки DataFrame
    pd.set_option('display.max_columns', None)

    #сайт, логин, пароль, безопасность
    client = Minio("127.0.0.1:9001", access_key="tlisova", secret_key="tlisova98", secure=False)

    #корзина и объект
    client.fget_object("test-bucket-201170", "201170.csv", "./test_csv.csv")

    #работа с файлом

    #with open("201170.csv", encoding="cp1251") as file:
    with open("test_csv.csv", encoding="cp1251") as file:

        csvframe = pd.read_csv(file, delimiter=';', header=None)

        # изменение поля Даты
        csvframe[0] = csvframe[0].map(lambda x: str(x)[-10:])

        # разбиение столбца
        csvframe[3] = csvframe[3].str.split(' ', 1, expand=True).values.tolist()
        s3 = pd.DataFrame(csvframe[3].values.tolist(), columns=['ooo', 'name'])
        # print(s3)

        # привести к числовому значению столбец
        csvframe[8] = pd.to_numeric(csvframe[8].apply(lambda x: re.sub(',', '.', str(x).replace(' ', ''))))

        # разбиение последнего столбца по годам
        csvframe[9] = csvframe[9].str.split('-', expand=True).values.tolist()
        s9 = pd.DataFrame(csvframe[9].values.tolist(), columns=['g1', 'g2'])

        # соединяем результаты
        res = pd.concat([csvframe, s3, s9], axis=1)

        # какие колонки в каком порядке будем сохранять
        res = res[[0, 1, 2, 'ooo', 'name', 4, 5, 6, 7, 8, 'g1', 'g2']]

        print(res)

        res.to_csv('new_201170.csv', index=False)


if __name__ == '__main__':
    main()
