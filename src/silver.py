from pyspark.sql.functions import *


def silver_data(df):


    # 1 Remove duplicates

    df = df.dropDuplicates()


    # 2 Trim strings

    for column_name, data_type in df.dtypes:

        if data_type == "string":

            df = df.withColumn(

                column_name,

                trim(col(column_name))

            )


    # 3 Lowercase order status

    df = df.withColumn(

        "order_status",

        lower(col("order_status"))

    )


    # 4 Replace null customer

    df = df.fillna({

        "customer_id": "UNKNOWN"

    })


    # 5 Filter quantity <=0

    df = df.filter(

        col("quantity") > 0

    )


    # 6 Load date

    df = df.withColumn(

        "load_date",

        current_date()

    )


    # 7 Order category

    df = df.withColumn(

        "order_category",

        when(

            col("order_amount") > 7000,

            "HIGH"

        )

        .when(

            col("order_amount") > 3000,

            "MEDIUM"

        )

        .otherwise(

            "LOW"

        )

    )


    return df