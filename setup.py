#!/usr/bin/env python
from setuptools import setup, find_packages
from saturn import __version__

long_description = open("README.md").read()

setup(
    name="saturn",
    version=__version__,
    packages=find_packages(),
    author="James Turk",
    author_email="jpturk@pbs.org",
    license="MIT",
    url="https://github.com/pbs/saturn/",
    description="tool for working with ECS tasks",
    long_description=long_description,
    platforms=["any"],
    zip_safe=False,
    entry_points="""[console_scripts]
  saturn = saturn.cli:cli""",
    install_requires=["Click==7.0", "tabulate==0.8.2", "boto3"],
    extras_require={"dev": ["black", "moto", "pytest"]},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
)
