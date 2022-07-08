"""Setup script for packaging the products_integrator."""

import sys
import site

from setuptools import setup
from product_integrator.version import version

site.ENABLE_USER_SITE = "--user" in sys.argv[1:]


setup(
    name="data-engineering-task",
    version=version,
    description="Spark Pipeline that transforms the supplier data so that it could be directly loaded into the target dataset without any other changes",
    author="oussama.louati4@gmail.com",
    entry_points={
        "console_scripts": ["product_integrator=product_integrator.main:main"]
    },
)
