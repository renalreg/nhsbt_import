from setuptools import setup, find_packages

setup(
    name='rr_ukt_import',
    author='UK Renal Registry',
    author_email='rrsystems@renalregistry.nhs.uk',
    url='https://www.renalreg.org/',
    packages=find_packages(),
    zip_safe=True,
    install_requires=[
        "sqlalchemy>=1.3, <2",
        "pyodbc>=4.0.30,<4.1; sys_platform == 'win32'",
        "rr.database @ git+https://github.com/renalreg/rr_database.git",
        "rr_common @ git+https://github.com/renalreg/rr_common.git",
        "rr_reports @ git+https://github.com/renalreg/rr_reports.git",
        "ukrr_models @ git+https://github.com/renalreg/ukrr_models.git",
    ],
)
