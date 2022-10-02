import glob
import pandas as pd
from sqlalchemy import create_engine

# TODO set host or move to parameters
CONNECTION_SOURCE_DB = 'postgresql://etl:etl_contest@host.docker.internal:54321/checknft'
CONNECTION_DEST_DB = 'postgresql://etl:etl_contest@host.docker.internal:54322/checknft'

CALCULATE_SELECT = """
WITH dt AS (
    SELECT generate_series((
                SELECT min(date) 
                FROM event):: date, now()::date, '1 day') ::date AS gen_date
),
event_prep_1 as (
    SELECT e."tokenId",
           e."eventType",
           e.date,
           to_date(e.date, 'YYYY-MM-DD') as tr_date,
           rank() OVER (PARTITION BY e."tokenId", 
                                     to_date(e.date, 'YYYY-MM-DD') 
                        ORDER BY date, 
                                 e."createdAt" DESC) AS date_rnk, 
           e."totalPrice",
           e."paymentTokenUSDPrice",
           pt.decimals,
           t."collectionId"
    FROM event e
    LEFT JOIN token t
         ON t.id = e."tokenId"
    LEFT JOIN payment_token pt
         ON e."paymentTokenId" = pt.id
    WHERE e."eventType" = 'sale'
),
event_prep_2 AS (
    SELECT ev."tokenId",
           ev."eventType",
           ev.date,
           ev.tr_date,
           ev."totalPrice",
           ev."paymentTokenUSDPrice",
           ev.decimals,
           ev."collectionId",
           ev."totalPrice"::decimal * pow(10, (-1 * ev.decimals)) * ev."paymentTokenUSDPrice" AS calc
     FROM event_prep_1 ev
     WHERE ev.date_rnk = 1
),
event_prep_3 AS (
    SELECT t.id AS tokenid,
           dt.gen_date,
           t."collectionId",
           c.name,
           ep.calc
    FROM dt
    CROSS JOIN token t
    LEFT JOIN event_prep_2 ep
        ON t.id = ep."tokenId"
        AND dt.gen_date = ep.tr_date
    LEFT JOIN collection c
        ON t."collectionId" = c.id
    ORDER BY gen_date DESC
),
event_prep_4 AS (
    SELECT tokenid,
           gen_date,
           "collectionId",
           name,
           CASE WHEN calc IS NULL THEN 0 ELSE calc END AS calc,
           CASE WHEN calc IS NOT NULL THEN 1 ELSE 0 END AS sale_flg
    FROM event_prep_3
    ORDER BY gen_date
),
event_prep_5 AS (
    SELECT tokenid,
           gen_date,
           "collectionId",
           name,
           calc,
           sale_flg,
           SUM(sale_flg) OVER (PARTITION BY tokenid 
                               ORDER BY gen_date rows between unbounded preceding and current row) AS sale_id
    FROM event_prep_4
),
event_prep_6 AS (
    SELECT tokenid,
           gen_date,
           "collectionId",
           name,
           Max(calc) OVER (PARTITION BY tokenid, 
                                        sale_id 
                           ORDER BY gen_date rows between unbounded preceding and current row) AS calc_new
    FROM event_prep_5
)
SELECT gen_date AS date,
       "collectionId" AS collectionId,
       name AS collectionId,
       SUM(calc_new) AS marketCapUSD
FROM event_prep_6
GROUP BY gen_date,
         "collectionId",
         name
ORDER BY gen_date,
         "collectionId";
"""


def download_csv_to_db_task():
    db = create_engine(CONNECTION_SOURCE_DB)
    csv_path = "../resources/"
    conn = db.connect()
    for filename in glob.glob(csv_path + "*.csv"):
        table_name = filename.replace("../resources/", "").replace(".csv", "")
        df = pd.read_csv(filename)
        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    conn.close()


def calculate_and_save_report_task():
    db1 = create_engine(CONNECTION_SOURCE_DB)

    conn = db1.connect()
    df = pd.read_sql(CALCULATE_SELECT, conn)
    conn.close()

    db2 = create_engine(CONNECTION_DEST_DB)

    conn = db2.connect()
    df.to_sql('report', con=conn, if_exists='replace', index=False)
    conn.close()
