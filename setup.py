# encoding: latin1
# import os
from setuptools import setup  # , find_packages

# BUILD_ID = os.environ.get("BUILD_BUILDID", "0")
setup(
    name="aws_databricks_toolkit",
    version="0.0.1",
    description="A simple library of functions for an aws databricks cloud.",
    # # Author details
    author="Max",
    # author_email="tdb",
    # packages=find_packages("src"),
    # package_dir={"": "src"},
    setup_requires=["boto3"],
    # tests_require=["pytest", "pytest-nunit", "pytest-cov"],
    # extras_require={"develop": ["pre-commit", "bump2version"]},
)
