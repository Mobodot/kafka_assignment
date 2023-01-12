from configparser import ConfigParser


def config_details(filename: str="config_file.ini", 
                   config_key: str=None) -> dict:
    config_parser = ConfigParser()
    config_parser.read_file(open(filename))
    config = dict(config_parser[config_key])
    return config