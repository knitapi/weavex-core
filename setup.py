from setuptools import setup, find_packages

setup(
    name="weavex-core",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # The critical addition for your new logger
        "google-cloud-bigquery>=3.10.0",

        # Existing dependencies likely needed by your other modules (state/storage)
        "requests>=2.31.0",
        "google-cloud-storage>=2.0.0", # Assuming you use this for object store
        # "temporalio>=1.4.0",         # Uncomment if weavex-core itself imports temporal types
    ],
    author="Knit",
    description="Core utilities for Weavex AI Agents and Sync Workers",
)