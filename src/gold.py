from pyspark.sql.functions import *

from pyspark.sql.window import Window


def gold_data(df):


    daily_revenue = (

        df.groupBy("order_date")

        .agg(

            sum("order_amount")

            .alias("daily_revenue")

        )

    )


    monthly_revenue = (

        df.withColumn(

            "month",

            date_format(

                "order_date",

                "yyyy-MM"

            )

        )

        .groupBy("month")

        .agg(

            sum("order_amount")

            .alias("monthly_revenue")

        )

    )


    customer_window = (

        Window

        .orderBy(

            desc("total_purchase")

        )

    )


    top_customers = (

        df.groupBy("customer_id")

        .agg(

            sum("order_amount")

            .alias("total_purchase")

        )

        .withColumn(

            "rank",

            dense_rank()

            .over(customer_window)

        )

        .filter(

            col("rank") <= 5

        )

    )


    product_window = (

        Window

        .orderBy(

            desc("total_sales")

        )

    )


    top_products = (

        df.groupBy("product_id")

        .agg(

            sum("order_amount")

            .alias("total_sales")

        )

        .withColumn(

            "rank",

            dense_rank()

            .over(product_window)

        )

        .filter(

            col("rank") <= 5

        )

    )


    revenue_window = (

        Window

        .orderBy("order_date")

        .rowsBetween(

            Window.unboundedPreceding,

            Window.currentRow

        )

    )


    running_revenue = (

        daily_revenue

        .withColumn(

            "running_revenue",

            sum("daily_revenue")

            .over(revenue_window)

        )

    )


    return {

        "daily_revenue": daily_revenue,

        "monthly_revenue": monthly_revenue,

        "top_customers": top_customers,

        "top_products": top_products,

        "running_revenue": running_revenue

    }