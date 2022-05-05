from setuptools import setup

packages = ["dbt", "dbt.adapters.doris", "dbt.include.doris"]

package_data = {
    "": ["*"],
    "dbt.include.doris": [
        "macros/adapters/*",
        "macros/get_custom_name/*",
        "macros/materializations/incremental/*",
        "macros/materializations/partition/*",
        "macros/materializations/seed/*",
        "macros/materializations/snapshot/*",
        "macros/materializations/table/*",
        "macros/materializations/view/*",
    ],
}

install_requires = ["dbt-core", "mysqlclient"]

setup_kwargs = {
    "name": "dbt-doris",
    "version": "0.1.0",
    "description": "The doris adapter plugin for dbt",
    "long_description": None,
    "maintainer": None,
    "maintainer_email": None,
    "url": None,
    "packages": packages,
    "package_data": package_data,
    "install_requires": install_requires,
    "python_requires": ">=3.7,<4.0",
}


setup(**setup_kwargs)
