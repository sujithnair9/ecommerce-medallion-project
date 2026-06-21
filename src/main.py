from pyspark.sql import SparkSession

from utils import load_config

from reader import read_data

from silver import silver_data

from dq import dq_job

from gold import gold_data

from writer import write_data


def main():

    spark = (

        SparkSession.builder

        .appName("ecommerce")

        .getOrCreate()

    )


    config = load_config(

        "/Workspace/Users/ssujithn400@gmail.com/ECOMMERCE_USECASE1/config/dev.yaml"

    )

    # Fix: Override incorrect delimiter - CSV file uses comma, not pipe
    config["source"]["delimiter"] = ","


    # Bronze

    bronze_df = read_data(

        config,

        spark

    )


    # DQ

    dq_df = dq_job(

        bronze_df,

        spark

    )


    write_data(

        dq_df,

        config["validation"]["dq_path"],

        config

    )


    # Silver

    silver_df = silver_data(

        bronze_df

    )


    write_data(

        silver_df,

        config["target"]["silver_path"],

        config

    )


    # Gold

    gold_dfs = gold_data(

        silver_df

    )


    for name, df in gold_dfs.items():

        output_path = (

            config["target"]["gold_path"]

            + "/"

            + name

        )


        write_data(

            df,

            output_path,

            config

        )


        print(name)

        df.show()


if __name__ == "__main__":

    main()
