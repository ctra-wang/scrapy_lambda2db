import http.client
import json
from datetime import datetime

import mysql.connector
import sqlite3

from common.db_config import DB_CONFIG
from common.generate_data_list import generate_date_list
from common.lambda_token import LAMBDA_TOKEN
from common.points import MOMENT_VALUES, MOMENT_HOUR_VALUES


def print_hi(today, dataFlag):
    print("-----------=-=-==-=")
    print(today)

    conn = http.client.HTTPSConnection("lambda.com.cn")
    payload = json.dumps({
        "province": "甘肃省",
        "date": today,
        "dataFlag": dataFlag
    })
    headers = {
        'Authorization': LAMBDA_TOKEN,
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Host': 'lambda.com.cn',
        'Connection': 'keep-alive',
        'Cookie': 'JSESSIONID=A82C767CB3F5AB892271B8CB9BDD3CF2'
    }
    conn.request("POST", "/gansu/basic/queryData/queryForecastAndRealPrice", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    json_data = json.loads(data.decode("utf-8"))
    if json_data["code"] == 400:
        print("赶紧换token吧您嘞！~~")
        return
    print(json_data)

    # Connect to the MySQL database
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # 如果
        timeType = 1
        arr = MOMENT_VALUES
        if dataFlag == 24:
            arr = MOMENT_HOUR_VALUES
            timeType = 2

        # "eastPriceAheadForecast": 392.47,
        # "eastPriceRealtimeForecast": 401.67,
        # "eastPriceAhead": 380.00,
        # "eastPriceRealtime": 370.00,
        # "westPriceAhead": 380.00,
        # "westPriceRealtime": 370.00,
        # "westPriceAheadForecast": 391.31,
        # "westPriceRealtimeForecast": 410.57

        addData = []
        # 循环96次（固定）
        for i in range(len(arr)):
            if json_data["data"][i]["infoTime"] == arr[i]:
                temparr = []
                print("----------")
                print(json_data["data"][i]["eastPriceAheadForecast"])
                temparr.append(json_data["data"][i]["eastPriceAheadForecast"])
                temparr.append(json_data["data"][i]["eastPriceRealtimeForecast"])
                temparr.append(json_data["data"][i]["eastPriceAhead"])
                temparr.append(json_data["data"][i]["eastPriceRealtime"])
                temparr.append(json_data["data"][i]["westPriceAhead"])
                temparr.append(json_data["data"][i]["westPriceRealtime"])
                temparr.append(json_data["data"][i]["westPriceAheadForecast"])
                temparr.append(json_data["data"][i]["westPriceRealtimeForecast"])
                temparr.append(today)
                temparr.append("甘肃省")
                temparr.append(arr[i])
                temparr.append(timeType)
                addData.append(temparr)

            # item.extend([today, MOMENT_VALUES[i]])
            # placeholders = ', '.join(['%s'] * len(item))
            # columns = ', '.join(
            #     ["intraday_price", "dayahead_price", "true_time_price", "province", "date", "moment"])
            #
            # insert_query = f"INSERT INTO lambda_tirline ({columns}) VALUES ({placeholders})"
            # # Execute the INSERT INTO statement
            # cursor.execute(insert_query, item)
            # conn.commit()
        # Connect to the MySQL database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 构建 SQL 插入语句
        insert_sql = "INSERT INTO lambda_area_forecast" \
                     " (east_dayahead_forecast_price, east_true_time_forecast_price, east_dayahead_price,east_true_time_price,west_dayahead_price,west_true_time_price,west_dayahead_forecast_price,west_true_time_forecast_price,date,province,moment,type)" \
                     " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        # 一次性插入多行数据
        cursor.executemany(insert_sql, addData)

        # 提交事务并关闭连接
        conn.commit()
        cursor.close()
        conn.close()


        print("Data successfully inserted into the database.")

    except mysql.connector.Error as err:
        print("Error: ", err)

    finally:
        cursor.close()
        conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dateList = generate_date_list(2023, 8)

    # dataFlag分为2个选项: 24 | 96
    dataFlag = [24, 96]

    for item in dateList:
        print('当前执行日期为：', item)
        print_hi(item, dataFlag[1])
