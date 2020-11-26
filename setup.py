from setuptools import setup, find_packages

def parse_req_line(line: str): -> str:
    package = line.split(";")[0]
    package = package.strip()
    
    return package

with open("requirements.txt") as f:
    install_req = [parse_req_line(x) for x in f.readlines() if not x.startswith("-e")]

setup(
    name='rr_ukt_import',
    #version=rr_foo.__version__,
    #long_description=rr_common.__doc__,
    author='UK Renal Registry',
    author_email='rrsystems@renalregistry.nhs.uk',
    url='https://www.renalreg.org/',
    packages=find_packages(),
    zip_safe=True,
    install_requires=install_req,
    #scripts=[
    #    'scripts/foo.py',
    #],
)
