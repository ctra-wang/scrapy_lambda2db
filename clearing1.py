import http.client
import json
import mysql.connector

from common.db_config import DB_CONFIG
from common.generate_data_list import generate_date_list
from common.lambda_token import LAMBDA_TOKEN
from common.points import MOMENT_VALUES



def print_hi(today, area):
    headers = {
        'Authorization': LAMBDA_TOKEN,
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Host': 'lambda.com.cn',
        'Connection': 'keep-alive',
        'Cookie': 'JSESSIONID=BF4DE3D727BB10A4E0B5DB62DE3AE0F9'
    }
    # url = "https://lambda.com.cn/gansu/basic/queryData/xxpl"
    conn = http.client.HTTPSConnection("lambda.com.cn")
    payload = json.dumps({
        "province": "甘肃省",
        "time": today,
        "area": area
    })
    # 请求接口
    conn.request("POST", "/gansu/basic/queryData/szggxx/queryByTimeAndArea", payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))
    # 返回结果
    json_data = json.loads(data.decode("utf-8"))
    if json_data["code"] == 400:
        print("赶紧换token吧您嘞！~~")
        return
    print(json_data)

    # Connect to the MySQL database
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        # 遍历96个时间点
        for i in range(len(MOMENT_VALUES)):
            province_value = "gansu"
            values = [area, today, province_value, MOMENT_VALUES[i],
                      json_data["data"][0]["data"].split(",")[i],
                      json_data["data"][1]["data"].split(",")[i]]
            # Prepare placeholders for the values
            placeholders = ', '.join(['%s'] * len(values))

            insert_query = f"INSERT INTO lambda_clearing (area,date, province,moment,true_time_price,dayahead_price) VALUES ({placeholders})"

            # Execute the INSERT INTO statement
            cursor.execute(insert_query, values)
            conn.commit()

        print("Data successfully inserted into the database.")

    except mysql.connector.Error as err:
        print("Error: ", err)

    finally:
        cursor.close()
        conn.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    date = generate_date_list(2022)

    # 甘肃区域划分： 河东 | 河西
    area = ["河东", "河西"]
    for item in date:
        print('当前执行日期为：', item)
        print_hi(item, area[1])
        # print_hi(item, area[1])


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
