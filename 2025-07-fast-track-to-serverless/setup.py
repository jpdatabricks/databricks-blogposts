from setuptools import setup, find_packages

# Read the contents of the README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="serverless-converter",
    version="0.1", 
    author="Luca Borin",
    author_email="luca.borin@databricks.com", 
    description="A utility to convert Databricks Declarative Pipelines to serverless compute and manage configurations.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(), 
    python_requires=">=3.8",
    install_requires=[
        "databricks-sdk==0.47.0",
    ],
    entry_points={
        "console_scripts": [
            "serverless-converter=serverless_converter.main:main",
        ],
    }
)