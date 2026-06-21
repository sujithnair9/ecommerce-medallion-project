from pyspark.sql.functions import *


def dq_job(df, spark):

    dq_results = []


    # 1 Unique order id

    duplicate_count = (

        df.groupBy("order_id")

        .count()

        .filter(

            col("count") > 1

        )

        .count()

    )


    status = "PASS"

    if duplicate_count > 0:

        status = "FAIL"


    dq_results.append({

        "TEST_NAME": "Unique Order Id",

        "FIELD_NAME": "order_id",

        "TEST_STATUS": status,

        "TEST_COUNT": duplicate_count

    })


    # 2 Customer id null

    customer_null_count = (

        df.filter(

            col("customer_id").isNull()

        ).count()

    )


    status = "PASS"

    if customer_null_count > 0:

        status = "FAIL"


    dq_results.append({

        "TEST_NAME": "Customer Id Not Null",

        "FIELD_NAME": "customer_id",

        "TEST_STATUS": status,

        "TEST_COUNT": customer_null_count

    })


    # 3 Order status validation

    valid_status = [

        "placed",

        "shipped",

        "delivered",

        "cancelled"

    ]


    invalid_status = (

        df.filter(

            ~lower(

                col("order_status")

            )

            .isin(valid_status)

        )

        .count()

    )


    status = "PASS"

    if invalid_status > 0:

        status = "FAIL"


    dq_results.append({

        "TEST_NAME": "Order Status Validation",

        "FIELD_NAME": "order_status",

        "TEST_STATUS": status,

        "TEST_COUNT": invalid_status

    })


    # 4 Quantity positive

    quantity_count = (

        df.filter(

            col("quantity") <= 0

        )

        .count()

    )


    status = "PASS"

    if quantity_count > 0:

        status = "FAIL"


    dq_results.append({

        "TEST_NAME": "Quantity Positive",

        "FIELD_NAME": "quantity",

        "TEST_STATUS": status,

        "TEST_COUNT": quantity_count

    })


    # 5 Order amount positive

    amount_count = (

        df.filter(

            col("order_amount") <= 0

        )

        .count()

    )


    status = "PASS"

    if amount_count > 0:

        status = "FAIL"


    dq_results.append({

        "TEST_NAME": "Order Amount Positive",

        "FIELD_NAME": "order_amount",

        "TEST_STATUS": status,

        "TEST_COUNT": amount_count

    })


    return spark.createDataFrame(

        dq_results

    )