from setuptools import find_packages, setup

import versioneer

with open("requirements.txt") as install_requires_file:
    install_requires = install_requires_file.read().strip().split("\n")

with open("requirements-dev.txt") as dev_requires_file:
    dev_requires = dev_requires_file.read().strip().split("\n")

with open("README.md") as readme_file:
    readme = readme_file.read()

setup(
    name="prefect-dbt",
    description="Prefect integration for working with dbt",
    license="Apache License 2.0",
    author="Prefect Technologies, Inc.",
    author_email="help@prefect.io",
    keywords="prefect",
    url="https://github.com/PrefectHQ/prefect-dbt",
    long_description=readme,
    long_description_content_type="text/markdown",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=("tests", "docs")),
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require={"dev": dev_requires},
    entry_points={
        "prefect.collections": [
            "DbtCloudCredentials = prefect_dbt.cloud.credentials",
            "DbtCliProfile = prefect_dbt.cli.credentials",
        ]
    },
    classifiers=[
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
    ],
)
