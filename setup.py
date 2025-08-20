#!/usr/bin/env python3

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

# Read requirements
requirements_path = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_path.exists():
    requirements = [
        line.strip()
        for line in requirements_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]

setup(
    name="sessionize",
    version="0.1.0",
    description="Enterprise Data Pipeline Framework for Apache Spark, Kafka, and Iceberg",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/sessionize",
    license="MIT",
    
    # Package discovery
    packages=find_packages(where=".", include=["src*"]),
    package_dir={"": "."},
    
    # Dependencies
    install_requires=requirements,
    python_requires=">=3.8",
    
    # Optional dependencies
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-cov>=6.0.0", 
            "black>=25.0.0",
            "flake8>=7.0.0",
            "mypy>=1.17.0",
            "coverage>=7.0.0"
        ],
        "all": [
            "jupyter>=1.0.0",
            "matplotlib>=3.0.0",
            "seaborn>=0.11.0"
        ]
    },
    
    # Entry points
    entry_points={
        "console_scripts": [
            "sessionize-validate=scripts.validate_setup:main",
            "sessionize-kafka-producer=scripts.kafka_producer:main",
        ],
    },
    
    # Classifiers
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators", 
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
    
    # Keywords
    keywords=[
        "apache-spark", "kafka", "iceberg", "data-pipeline", "streaming", 
        "batch-processing", "etl", "data-lake", "big-data", "pyspark"
    ],
    
    # Include additional files
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt", "*.yaml", "*.yml", "*.json"],
    },
    
    # Project URLs
    project_urls={
        "Bug Reports": "https://github.com/yourusername/sessionize/issues",
        "Source": "https://github.com/yourusername/sessionize",
        "Documentation": "https://github.com/yourusername/sessionize/wiki",
    },
)