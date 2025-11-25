from setuptools import setup, find_packages

setup(
    name="datavision-easy-store",
    version="0.2.0",
    description="High-performance file archiving system for S3/HCP",
    author="Dariusz Duszyński",
    author_email="dariusz@datavision.pl",
    url="https://github.com/dariuszduszynski/Datavision-Easy-Store",
    
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    
    install_requires=[
        "boto3>=1.26.0",
        "psycopg2-binary>=2.9.0",
        "asyncpg>=0.27.0",
        "redis>=4.5.0",
        "pydantic>=2.0.0",
        "fastapi>=0.103.0",
        "uvicorn>=0.23.0",
        "click>=8.1.0",
        "pyyaml>=6.0",
        "prometheus-client>=0.16.0",
        "SQLAlchemy>=2.0.0",
    ],
    
    extras_require={
        "dev": [
            "pytest>=7.3.0",
            "pytest-cov>=4.0.0",
            "black>=23.3.0",
            "mypy>=1.3.0",
            "ruff>=0.0.270",
        ],
        "mysql": ["pymysql>=1.0.0"],
        "mssql": ["pymssql>=2.2.0"],
    },
    
    entry_points={
        "console_scripts": [
            "des=des.cli.main:cli",
            "des-name-assignment=scripts.run_name_assignment:main",
            "des-packer=scripts.run_packer:main",
        ],
    },
    
    python_requires=">=3.11",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
    ],
)
