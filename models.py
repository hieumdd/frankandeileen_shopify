import os
from datetime import datetime

import requests
from google.cloud import bigquery

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

BQ_CLIENT = bigquery.Client()
DATASET = "Shopify"

API_VER = "2021-07"
SHOP_URL = "frankandeileen.myshopify.com"


class OrderLines:
    keys = {
        "p_key": [
            "id",
        ],
        "incre_key": "updated_at",
    }
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "order_number", "type": "INTEGER"},
        {"name": "created_at", "type": "TIMESTAMP"},
        {"name": "updated_at", "type": "TIMESTAMP"},
        {"name": "email", "type": "STRING"},
        {
            "name": "line_items",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {"name": "id", "type": "INTEGER"},
                {"name": "name", "type": "STRING"},
                {"name": "price", "type": "NUMERIC"},
                {
                    "name": "price_set",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "shop_money",
                            "type": "RECORD",
                            "fields": [
                                {"name": "amount", "type": "NUMERIC"},
                                {"name": "currency_code", "type": "STRING"},
                            ],
                        },
                        {
                            "name": "presentment_money",
                            "type": "RECORD",
                            "fields": [
                                {"name": "amount", "type": "NUMERIC"},
                                {"name": "currency_code", "type": "STRING"},
                            ],
                        },
                    ],
                },
                {"name": "quantity", "type": "INTEGER"},
                {"name": "sku", "type": "STRING"},
                {"name": "total_discount", "type": "NUMERIC"},
                {
                    "name": "total_discount_set",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "shop_money",
                            "type": "RECORD",
                            "fields": [
                                {"name": "amount", "type": "NUMERIC"},
                                {"name": "currency_code", "type": "STRING"},
                            ],
                        },
                        {
                            "name": "presentment_money",
                            "type": "RECORD",
                            "fields": [
                                {"name": "amount", "type": "NUMERIC"},
                                {"name": "currency_code", "type": "STRING"},
                            ],
                        },
                    ],
                },
            ],
        },
    ]

    def __init__(self, start, end):
        self.table = self.__class__.__name__
        self.start, self.end = self.get_time_range(start, end)

    def get_time_range(self, start, end):
        if start and end:
            _start, _end = [datetime.strptime(i, "%Y-%m-%d") for i in [start, end]]
        else:
            _end = datetime.utcnow()
            query = f"""
            SELECT MAX({self.keys['incre_key']}) AS incre
            FROM `{DATASET}`.`{self.table}`
            """
            rows = BQ_CLIENT.query(query).result()
            _start = [dict(row) for row in rows][0]["incre"].replace(tzinfo=None)
        return _start, _end

    def _get(self, session, url=None, params=None):
        url = (
            f"https://{os.getenv('API_KEY')}:{os.getenv('API_SECRET')}@{SHOP_URL}/admin/api/{API_VER}/orders.json"
            if not url
            else url
        )
        params = (
            {
                "limit": 250,
                "status": "any",
                "updated_at_min": self.start.isoformat(timespec='seconds'),
                "updated_at_max": self.end.isoformat(timespec='seconds'),
                "fields": ",".join(
                    [
                        "id",
                        "order_number",
                        "email",
                        "created_at",
                        "line_items",
                        "updated_at",
                    ]
                ),
            }
            if not params
            else params
        )
        with session.get(
            url,
            params=params,
        ) as r:
            res = r.json()
            if r.links.get("next", None):
                next_ = self._get(
                    session,
                    url=r.links["next"]["url"].replace(
                        SHOP_URL,
                        f"{os.getenv('API_KEY')}:{os.getenv('API_SECRET')}@{SHOP_URL}",
                    ),
                    params={
                        "limit": 250,
                    },
                )
            else:
                next_ = []
            return res["orders"] + next_

    def _transform(self, rows):
        return [
            {
                "id": row["id"],
                "order_number": row["order_number"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "email": row["email"],
                "line_items": [
                    {
                        "id": line_item["id"],
                        "name": line_item["name"],
                        "price": line_item["price"],
                        "price_set": line_item["price_set"],
                        "quantity": line_item["quantity"],
                        "sku": line_item["sku"],
                        "total_discount": line_item["total_discount"],
                        "total_discount_set": line_item["total_discount_set"],
                    }
                    for line_item in row["line_items"]
                ]
                if row.get("line_items", [])
                else [],
            }
            for row in rows
        ]

    def _load(self, rows):
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{DATASET}.{self.table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=self.schema,
                ),
            )
            .result()
            .output_rows
        )
        self._update()
        return output_rows

    def _update(self):
        """Update the main table using the staging table"""

        query = f"""
        CREATE OR REPLACE TABLE `{DATASET}`.{self.table} AS
        SELECT * EXCEPT (row_num) FROM
        (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY {','.join(self.keys['p_key'])}
                ORDER BY {self.keys['incre_key']} DESC
            ) AS row_num
        FROM
            `{DATASET}`.`{self.table}`
        )
        WHERE row_num = 1
        """
        BQ_CLIENT.query(query).result()

    def run(self):
        """Run the job

        Returns:
            dict: Job's Results
        """

        with requests.Session() as session:
            rows = self._get(session)
        response = {
            "table": self.table,
            "start": self.start.isoformat(timespec="seconds"),
            "end": self.end.isoformat(timespec="seconds"),
            "num_processed": len(rows),
        }
        if rows:
            rows = self._transform(rows)
            response["output_rows"] = self._load(rows)
        return response


x = OrderLines("2021-09-01", "2021-09-03")
y = x.run()
y
