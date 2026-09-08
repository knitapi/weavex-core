from setuptools import setup, find_packages

setup(
    name="weavex-core",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # BigQuery
        "google-cloud-bigquery>=3.10.0",

        # State / storage
        "requests>=2.31.0",
        "google-cloud-storage>=2.0.0",

        # Database
        "psycopg2-binary>=2.9.10",
        "snowflake-connector-python>=4.5.0",

        # LangChain
        "langchain-core>=1.6.1",
        "langchain-google-genai>=4.4.0",
        "langchain-openai>=1.6.0",
        "langchain-anthropic>=1.5.2",

        # "temporalio>=1.4.0",  # Uncomment if weavex-core imports temporal types
    ],
    author="Knit",
    description="Core utilities for Weavex AI Agents and Sync Workers",
)