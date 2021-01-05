# 1.连接测试库，查询表空间信息，将数据写入字符串里
import json  # 导入依赖库
import requests
import schedule
import urllib.parse
import base64
import hashlib
import hmac
import time
import cx_Oracle


def get_data():
    conn = cx_Oracle.connect('username/password@ip:port/orcl')
    print(conn)
    curs = conn.cursor()
    sql = 'select a.tablespace_name,a.bytes / 1024 / 1024 "sum MB",round((a.bytes - b.bytes) / 1024 / 1024,2) "used MB",round(b.bytes / 1024 / 1024,2) "free MB",'
    sql += 'round(((a.bytes - b.bytes) / a.bytes) * 100, 2) "percent_used" from (select tablespace_name, sum(bytes) bytes from dba_data_files group by tablespace_name) a,'
    sql += '(select tablespace_name, sum(bytes) bytes, max(bytes) largest from dba_free_space group by tablespace_name) b where a.tablespace_name = b.tablespace_name'
    sql += ' and a.tablespace_name like \'JMS%\'order by ((a.bytes - b.bytes) / a.bytes) desc'
    curs.execute(sql)
    # 返回查询结果
    data = curs.fetchmany()
    curs.close()
    conn.close()
    # 新增一个字符串,存放表空间查询数据
    tablespace_str = 'tablespace\t\tsum_MB\t\tused_MB\t\tfree_MB\t\tpercent_used\n'
    # 循环查询结果的数组
    for i in data:
        tablespace_str += '\n'
        # 再循环数组中子元组
        for j in i:
            tablespace_str += str(j)
            tablespace_str += '\t\t\t'
    # print(tablespace_str)
    return tablespace_str


# 2.获取机器人的时间戳与签名


def job():
    timestamp = str(round(time.time() * 1000))
    secret = 'SEC63240c3f1b4e015f68f544f884e8cc738f2f7783208d8b32ddbfbde7ec9f85d2'
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(
        secret_enc,
        string_to_sign_enc,
        digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    # print(timestamp)
    # print(sign)

    # 获取当前时间点数据字符串
    curr_str = get_data() + '\n\nPlease help monitor more than 90% of the table space，thank you！'
    # print(curr_str)

    # 3.生成Post请求参数，并且发送请求
    headers = {'Content-Type': 'application/json'}  # 定义数据类型
    webhook = 'https://oapi.dingtalk.com/robot/send?access_token=6357d13f4d4529e41fbcdb317db9f6540cd72e55fb8b308ae69d05e8102e3d39&timestamp=' + timestamp + "&sign=" + sign
    # 定义要发送的数据
    data = {
        "msgtype": "text",
        "text": {"content": curr_str},
        "at": {
                "atMobiles": [],
                "isAtAll": True}
    }
    res = requests.post(
        webhook,
        data=json.dumps(data),
        headers=headers)  # 发送post请求

    # 当前时间
    curr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(res.text + '\t' + curr)


schedule.every(2).hours.do(job)  # 每隔2h执行一次任务

while True:
    schedule.run_pending()
    time.sleep(1)
