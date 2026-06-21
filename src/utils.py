import yaml


def load_config(path):

    try:

        with open(path, "r") as file:

            config = yaml.safe_load(file)

        return config

    except Exception as e:

        print("Error loading yaml")

        print(e)
