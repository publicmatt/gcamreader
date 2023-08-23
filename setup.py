from setuptools import setup, find_packages

VERSION = "1.3.0"

def readme():
    """Return the contents of the project README file."""
    with open('README.md') as f:
        return f.read()

setup(
    name="gcamreader",
    version=VERSION,
    python_requires=">=3.9",
    packages=find_packages(),
    description="Tools for importing GCAM output data",
    long_description=readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/JGCRI/gcamreader",
    license='BSD 2-Clause',
    author="Robert Link",
    author_email="robert.link@pnnl.gov",
    install_requires=[
        "requests~=2.20.0",
        "pandas>=1.2.4",
        "lxml>=4.6.3"
        "click>=8.1.6",
        "click-default-group>=1.2.3",
    ],
    entry_points="""
        [console_scripts]
        gcamreader=gcamreader.cli:cli
    """,
    include_package_data=True,
    zip_safe=False
)
