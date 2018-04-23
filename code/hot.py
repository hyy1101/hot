# coding=utf-8
import pandas as pd
import numpy as np
import MySQLdb
import time
import datetime
from sshtunnel import SSHTunnelForwarder

# SSH参数配置
SSH_HOST = "115.29.170.4"
SSH_PORT = 10022
SSH_USER = "git_deploy"
SSH_KEY = "/Users/hyy/.ssh/id_rsa"

# 数据库参数配置
MYSQL_HOST = "rm-bp161c6dttm306835.mysql.rds.aliyuncs.com"
MYSQL_PORT = 3306
MYSQL_USER = "bi_r"
MYSQL_PASSWORD = "ruhnn123!@#"
MYSQL_DB = "bi_data"


# 连接数据库
def ssh_connect_and_read_db(sql):
    with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=SSH_KEY,
            remote_bind_address=(MYSQL_HOST, MYSQL_PORT)
    ) as server:
        conn = MySQLdb.connect(host='127.0.0.1',
                               port=server.local_bind_port,
                               user=MYSQL_USER,
                               passwd=MYSQL_PASSWORD,
                               db=MYSQL_DB,
                               charset="utf8")
        cur = conn.cursor()
        count = cur.execute(sql)
        print("获取数据条数:{}".format(count))
        data = cur.fetchall()
        df = pd.DataFrame(list(data))
        return df


def ssh_connect_and_delete_table(delete_sql):
    with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=SSH_KEY,
            remote_bind_address=(MYSQL_HOST, MYSQL_PORT)
    ) as server:
        conn = MySQLdb.connect(host='127.0.0.1',
                               port=server.local_bind_port,
                               user="bi_rw",
                               passwd="Ruhnn!@#123",
                               db=MYSQL_DB,
                               charset="utf8")
        cur = conn.cursor()
        count = cur.execute(delete_sql)
        conn.commit()

        print("删除数据成功！")
        print("影响数据条数:{}".format(count))


def ssh_connect_and_insert_table(insert_sql, df):
    tuple_of_df = tuple(np.array(df).tolist())
    with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=SSH_KEY,
            remote_bind_address=(MYSQL_HOST, MYSQL_PORT)
    ) as server:
        conn = MySQLdb.connect(host='127.0.0.1',
                               port=server.local_bind_port,
                               user="bi_rw",
                               passwd="Ruhnn!@#123",
                               db=MYSQL_DB,
                               charset="utf8")

        cur = conn.cursor()
        count = cur.executemany(insert_sql, tuple_of_df)
        conn.commit()
        print("插入数据成功！")
        print("影响数据条数:{}".format(count))


# 计算前一天上线首日销量
RANGE = 2000
yesterday = datetime.date.today() - datetime.timedelta(days=1)
start_time = time.time()
df_date = pd.date_range('2017-10-23', yesterday)
for dt in df_date[-1:]:
    dt2 = dt + datetime.timedelta(1)
    sql = '''
    select 
        distinct a.* 
    from 
        (select 
            *
        from 
            tb_item 
        where 
            status=1 and 
            first_day_sale > 100 and sale_time >= '%s' and sale_time < '%s'
        ) a 
    inner join 
        (select shop_id, count(distinct item_id) as product_num 
        from 
            tb_item 
        where 
            status=1 and             
            sale_time >= '%s' and sale_time < '%s' 
        group by shop_id 
        )b 
    on a.shop_id = b.shop_id 
    where 
        product_num > 4 
    order by a.shop_id, a.first_day_sale desc;''' % (dt, dt2, dt, dt2)

    df_first_sale = ssh_connect_and_read_db(sql)

    if len(df_first_sale) == 0:
        continue

    print(dt)
    df_first_sale.columns = (['id', 'daily_record_id', 'item_id', 'title', 'pic_url', 'pic_url_list', 'properties',
                              'sale_time', 'init_sale_time', 'category_id', 'category_name', 'shop_id', 'shop_name',
                              'hot_score', 'recommended_hot', 'first_day_sale', 'first_day_sale_amount',
                              'first_week_sale', 'first_week_sale_amount', 'first_month_sale',
                              'first_month_sale_amount', 'total_sale', 'total_sale_amount', 'last_month_sale',
                              'avg_day_sale', 'cur_cprice', 'cur_sprice', 'collect', 'comment_count', 'status',
                              'created_at', 'updated_at', 'deleted_at', 'version'])
    df_first_sale['hot_score'] = df_first_sale.groupby(['shop_id'])['first_day_sale'].rank(ascending=False).astype(int)

    # print(df_first_sale.shape)
    delete_sql = ('''delete from tb_item where id in (%s);''' % ','.join(df_first_sale['id'].astype(str).tolist()))
    ssh_connect_and_delete_table(delete_sql)
    insert_sql = '''insert into tb_item values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    if len(df_first_sale) < RANGE:
        ssh_connect_and_insert_table(insert_sql, df_first_sale)
    else:
        for i in range(0, len(df_first_sale), RANGE):
            start = i
            end = min(i + RANGE, len(df_first_sale))
            df_part = df_first_sale.iloc[start:end, :].values
            ssh_connect_and_insert_table(insert_sql, df_part)
    print("运行时间:{:.4f}s".format(time.time() - start_time))
