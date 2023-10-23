from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable, Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from lib import ConnectionBuilder, MongoConnect
from pydantic import BaseModel
from typing import Dict, Optional
from psycopg.rows import class_row
from lib import PgConnect
from lib.dict_util import json2str

PG_ORIGIN_BONUS_SYSTEM_CONNECTION = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
PG_WAREHOUSE_CONNECTION = "PG_WAREHOUSE_CONNECTION"

dag = DAG(
    dag_id='load_stg',
    schedule_interval=timedelta(minutes=160),
    start_date=datetime(2022, 5, 5),
    catchup=False
)

PG_HOOK_SRC = PostgresHook(postgres_conn_id=PG_ORIGIN_BONUS_SYSTEM_CONNECTION)
PG_HOOK_DEST = PostgresHook(postgres_conn_id=PG_WAREHOUSE_CONNECTION)



class UsersReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_users(self, load_threshold: datetime, limit):
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {'update_ts': {'$gt': load_threshold}}

        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        sort = [('update_ts', 1)]

        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(self.dbs.get_collection("users").find(filter=filter, sort=sort, limit=limit))
        return docs
    
class OrdersReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_orders(self, load_threshold: datetime, limit):
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {'update_ts': {'$gt': load_threshold}}

        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        sort = [('update_ts', 1)]

        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(self.dbs.get_collection("orders").find(filter=filter, sort=sort, limit=limit))
        return docs

class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict
    
class StgEtlSettingsRepository:
    def get_setting(self, conn: Connection, etl_key: str, schema: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                f"""
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM {schema}.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str, schema: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO {schema}.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )


# Получаем переменные из Airflow.
cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
rs = Variable.get("MONGO_DB_REPLICA_SET")
db = Variable.get("MONGO_DB_DATABASE_NAME")
host = Variable.get("MONGO_DB_HOST")


def load_ranks_callable(**context):
    sql = """
    select * from ranks; 
    """
    records = PG_HOOK_SRC.get_records(sql)
    conn = PG_HOOK_DEST.get_conn()
    
    with conn:
        with conn.cursor() as curs:
            sql = """
            insert into stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold)
            values (%s, %s, %s, %s);
            """
            curs.executemany(sql, records)


def load_users_callable(**context):
    WF_KEY = "bonussystem_users"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="stg")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: 0
                }
            )
    
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = int(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
    
    
        sql = f"""
        select * from users
        where id > {last_loaded_ts}
        order by id
        """
        records = PG_HOOK_SRC.get_records(sql)
    
        with conn.cursor() as curs:
            for record in records:
                
                    sql = """
                    insert into stg.bonussystem_users (id, order_user_id)
                    values (%s, %s);
                    """
                    curs.execute(sql, record)
                    
        if records:
            wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = records[-1][0]
            wf_setting_json = str(wf_setting.workflow_settings)
            wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
        
                
def load_events_callable(**context):
    WF_KEY = "bonussystem_event"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="stg")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )
    
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        
        sql = f"""
        select * from outbox
        where event_ts > '{last_loaded_ts}'
        order by event_ts ASC; 
        """
        records = PG_HOOK_SRC.get_records(sql)
        print(f"{records=}")
        
        for record in records:
                with conn.cursor() as curs:
                    sql = """
                    INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                    VALUES (%s, %s, %s, %s)
                    """
                    curs.execute(sql, record)
                    
        if records:
            wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = records[-1][1]
            print(f'{wf_setting.workflow_settings[LAST_LOADED_TS_KEY]=}')
            wf_setting_json = json2str(wf_setting.workflow_settings)
            print(f'{wf_setting_json=}')
            wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='stg')
            
def load_user_ordersystem_callable(**context):
    WF_KEY = "ordersystem_user"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="stg")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )
            
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        print(f'last_loaded_ts_str=')
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        
        users = UsersReader(mongo_connect).get_users(last_loaded_ts, 1000)
        print(users, "<---")
        
        for user in users:
            str_val = json2str(user)
            
            with conn.cursor() as cur:
                    sql = """
                        INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """
                    param =  {
                        "id": str(user["_id"]),
                        "val": str_val,
                        "update_ts": user["update_ts"]
                    }
                    cur.execute(sql, param)
        
        if users:
            wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in users])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='stg')
        
        
def load_orders_ordersystem_callable(**context):
    WF_KEY = "ordersystem_orders"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema='stg')
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )
            
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        
        orders = OrdersReader(mongo_connect).get_orders(last_loaded_ts, 1000)
        print(orders, "<---")
        
        for order in orders:
            str_val = json2str(order)
            
            with conn.cursor() as cur:
                    sql = """
                        INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """
                    param =  {
                        "id": str(order["_id"]),
                        "val": str_val,
                        "update_ts": order["update_ts"]
                    }
                    cur.execute(sql, param)

        
        
        if orders:
            wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in orders])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='stg')
        





def load_dds_users_callable(**context):
    WF_KEY = "dds_users"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="dds")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )

            
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        with conn.cursor() as cur:
            sql=f"""
            SELECT object_value::JSON->>'_id', object_value::JSON->>'login', object_value::JSON->>'name', object_value::JSON->>'update_ts'
            from stg.ordersystem_users
            where update_ts > '{last_loaded_ts}'
            order by (object_value::JSON->>'update_ts') ASC;
            """
            
            cur.execute(sql)
            users = cur.fetchall() 
            print(users, "<--- users")
            
            for user in users:
                
                sql = """
                INSERT INTO dds.dm_users (user_id, user_login, user_name)
                VALUES (%s, %s, %s)
                """
                cur.execute(sql, [user[0], user[1], user[2]])
                
            if users:
                wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = users[-1][3]
                wf_setting_json = json2str(wf_setting.workflow_settings)
                wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='dds')
                
def load_dds_rest_callable(**context):
    WF_KEY = "dds_rest"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="dds")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )

            
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        with conn.cursor() as cur:
            sql=f"""
            SELECT object_value::JSON->>'_id', object_value::JSON->>'name', update_ts
            from stg.ordersystem_restaurants
            where update_ts > '{last_loaded_ts}'
            order by update_ts ASC;
            """
            
            cur.execute(sql)
            rests = cur.fetchall() 
            print(rests, "<--- users")
            
            for rest in rests:
                
                sql = """
                INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                VALUES (%s, %s, %s, %s)
                """
                cur.execute(sql, [rest[0], rest[1], rest[2], '2099-12-31 00:00:00.000'])
                
            if rests:
                wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = rests[-1][2]
                wf_setting_json = json2str(wf_setting.workflow_settings)
                wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='dds')
                
                
                
def load_dds_timestamp_callable(**context):
    WF_KEY = "dds_time_stamp"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="dds")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )

            
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        with conn.cursor() as cur:
            sql=f"""
            SELECT DISTINCT
                update_ts::timestamp as ts
            from stg.ordersystem_orders
            where object_value::JSON->>'final_status' in ('CLOSED', 'CANCELLED') and update_ts > '{last_loaded_ts}'
            order by ts ASC;
            """
            
            cur.execute(sql)
            timestamps = cur.fetchall() 
            print(timestamps, "<--- timestamps")
            
            for timestamp in timestamps:

                timestamp = timestamp[0]
                
                sql = """
                INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                print(timestamp, timestamp.year, timestamp.month, timestamp.day, timestamp.time(), timestamp.date(), "<------------")
                cur.execute(sql, [timestamp, timestamp.year, timestamp.month, timestamp.day, timestamp.time(), timestamp.date()])
                
            if timestamps:
                wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = timestamp
                wf_setting_json = json2str(wf_setting.workflow_settings)
                wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='dds')
                
                
def load_dds_products_callable(**context):
    WF_KEY = "dds_products"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="dds")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )

            
        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        with conn.cursor() as cur:
            sql=f"""
            with kek as (
            select distinct
                object_id,
                json_array_elements(object_value::JSON->'menu')->>'_id' as product_id, 
                json_array_elements(object_value::JSON->'menu')->>'name' as product_name,
                (json_array_elements(object_value::JSON->'menu')->>'price')::numeric  as product_price,
                update_ts as active_from,
                '2099-12-31 00:00:00.000'::timestamp as active_to
            from stg.ordersystem_restaurants or2)
            select product_id, product_name, product_price, kek.active_from, kek.active_to, dmp.id as restaurant_id from kek
            join dds.dm_restaurants dmp on dmp.restaurant_id = kek.object_id
            where kek.active_from > '{last_loaded_ts}'
            order by kek.active_from ASC
            """
            
            cur.execute(sql)
            products = cur.fetchall() 
            print(products, "<--- products")
            
            for product in products:
                
                sql = """
                INSERT INTO dds.dm_products (product_id, product_name, product_price, active_from, active_to, restaurant_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql, product)
                
            if products:
                wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = products[-1][3]
                wf_setting_json = json2str(wf_setting.workflow_settings)
                wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='dds')
                
                
def load_dds_orders_callable(**context):
    WF_KEY = "dds_orders"
    wf = StgEtlSettingsRepository()
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    with dwh_pg_connect.connection() as conn:
        wf_setting = wf.get_setting(conn, WF_KEY, schema="dds")
        LAST_LOADED_TS_KEY = "last_loaded_ts"
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=WF_KEY,
                workflow_settings={
                    LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )

        last_loaded_ts_str = wf_setting.workflow_settings[LAST_LOADED_TS_KEY]
        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
        print(f"starting to load from last checkpoint: {last_loaded_ts}")
        
        with conn.cursor() as cur:
            sql=f"""
            select
                du.id as user_id,
                dr.id as restaurant_id,
                dt.id as  timestamp_id,
                object_id as order_key,
                object_value::JSON->>'final_status' as order_status,
                oo.update_ts
            from stg.ordersystem_orders oo
            join dds.dm_users du on du.user_id = (oo.object_value::JSON->>'user')::JSON->>'id'
            join dds.dm_restaurants dr on dr.restaurant_id = (oo.object_value::JSON->>'restaurant')::JSON->>'id'
            join dds.dm_timestamps dt on dt.ts = oo.update_ts::timestamp
            where oo.update_ts  > '{last_loaded_ts}'
            order by oo.update_ts ASC
            """
            
            cur.execute(sql)
            orders = cur.fetchall() 
            print(orders, "<--- products")
            
            for order in orders:
                
                sql = """
                INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
                VALUES (%s, %s, %s, %s, %s)
                """
                cur.execute(sql, order[:-1])
                
            if orders:
                wf_setting.workflow_settings[LAST_LOADED_TS_KEY] = str(orders[-1][5])
                wf_setting_json = json2str(wf_setting.workflow_settings)
                wf.save_setting(conn, wf_setting.workflow_key, wf_setting_json, schema='dds')
        
        
    

load_ranks = PythonOperator(
    task_id='load_ranks',
    python_callable=load_ranks_callable,
    dag=dag
)

load_users = PythonOperator(
    task_id='load_users',
    python_callable=load_users_callable,
    dag=dag
)

load_events = PythonOperator(
    task_id='load_events',
    python_callable=load_events_callable,
    dag=dag
)

load_users_order_system = PythonOperator(
    task_id='load_user_ordersystem',
    python_callable=load_user_ordersystem_callable,
    dag=dag,
    trigger_rule='one_success'
)

load_orders_order_system = PythonOperator(
    task_id='load_orders_oredersystem',
    python_callable=load_orders_ordersystem_callable,
    dag=dag
)

load_dds_users = PythonOperator(
    task_id='load_dds_users',
    python_callable=load_dds_users_callable,
    dag=dag
)

load_dds_rest = PythonOperator(
    task_id='load_dds_rest',
    python_callable=load_dds_rest_callable,
    dag=dag
)

load_dds_timestamp = PythonOperator(
    task_id='load_dds_timestamp',
    python_callable=load_dds_timestamp_callable,
    dag=dag
)

load_dds_products = PythonOperator(
    task_id='load_dds_products',
    python_callable=load_dds_products_callable,
    dag=dag
)

load_dds_orders = PythonOperator(
    task_id='load_dds_orders',
    python_callable=load_dds_orders_callable,
    dag=dag
)

(
    [load_ranks, load_users, load_events] >>
    load_users_order_system >> 
    load_orders_order_system >> 
    load_dds_users >> 
    load_dds_rest >> 
    load_dds_timestamp >>
    load_dds_products >> 
    load_dds_orders
)
