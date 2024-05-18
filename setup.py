""" DAGS SETUP """

from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'ETL METACRITICS GAMES'
LONG_DESCRIPTION = 'ETL about metactitic data games'

setup(
    name="dags",
    version=VERSION,
    author="ETL GROUP",
    author_email="dvs.mazuera@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[],
    keywords=['python', 'primer paquete'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
