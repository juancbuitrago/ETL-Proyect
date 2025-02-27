"""Connection Settings"""
from configparser import ConfigParser


def config(filename="database.ini", section="postgresql"):
    """create a parser"""
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
        return db
    else:
        raise ImportError(f"Section '{section}' not found in the '{filename}' file")
