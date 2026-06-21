def read_data(config, spark):

    try:

        df = (

            spark.read

            .format(

                config["source"]["format"]

            )

            .option(

                "header",

                config["source"]["header"]

            )

            .option(

                "delimiter",

                config["source"]["delimiter"]

            )

            .load(

                config["source"]["file_path"]

            )

        )

        print("Bronze data loaded")

        return df

    except Exception as e:

        print("Error reading source file")

        print(e)