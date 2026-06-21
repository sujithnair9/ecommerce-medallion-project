def write_data(df, path, config):

    try:

        (

            df.write

            .format(

                config["target"]["format"]

            )

            .mode(

                config["target"]["output_mode"]

            )

            .option(

                "header",

                config["target"]["header"]

            )

            .save(path)

        )


        print(

            f"Data written successfully -> {path}"

        )


    except Exception as e:

        print(

            "Error writing data"

        )

        print(e)