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
        "google-cloud-firestore>=2.16.0", # imported by state.py and dao.py
        "google-cloud-pubsub>=2.19.0",  # imported by events.py and logging_utils/transports.py

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