from setuptools import setup, find_packages

setup(
    name='rr_ukt_import',
    #version=rr_foo.__version__,
    #long_description=rr_common.__doc__,
    author='UK Renal Registry',
    author_email='rrsystems@renalregistry.nhs.uk',
    url='https://www.renalreg.org/',
    packages=find_packages(),
    zip_safe=True,
    install_requires=[
        'click',
        'python-dateutil',
    ],
    #scripts=[
    #    'scripts/foo.py',
    #],
)
