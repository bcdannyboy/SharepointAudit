from setuptools import setup, find_packages

setup(
    name="sharepoint-audit",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "Office365-REST-Python-Client>=2.6.0",
        "msgraph-sdk>=1.0.0",
        "azure-identity>=1.14.0",
        "click>=8.0.0",
        "aiohttp>=3.8.0",
        "aiosqlite>=0.19.0",
        "streamlit>=1.28.0",
        "pandas>=2.0.0",
        "plotly>=5.0.0",
        "sqlalchemy>=2.0.0",
        "python-dateutil>=2.8.0",
        "tqdm>=4.65.0",
        "cachetools>=5.3.0",
        "tenacity>=8.2.0",
        "cryptography>=41.0.0",
        "psutil>=5.9.0",
        "pyyaml>=6.0.0",
        "rich>=13.0.0",
        "streamlit-aggrid>=0.3.4",
        "prometheus-client>=0.18.0",
        "python-json-logger>=2.0.7",
        "redis>=5.0.0",
        "humanize>=4.9.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "pre-commit>=3.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "sharepoint-audit=cli.main:main",
        ],
    },
    python_requires=">=3.11",
)
