import re
from pathlib import Path
from setuptools import setup, find_packages

# Single source of truth: Unifier.__version__ in unifier/unifier.py — the same
# string the client sends as client_version on the wire. Read, not imported:
# setup must not execute the package (its deps aren't installed at build time).
_version = re.search(
    r"__version__\s*=\s*['\"]([^'\"]+)['\"]",
    (Path(__file__).parent / 'unifier' / 'unifier.py').read_text(),
).group(1)

setup(
    name='unifier',
    version=_version,
    packages=find_packages(),
    install_requires=[
        'requests',
        'urllib3>=2',   # HTTPResponse.read1 (streaming reads)
        'ijson',
        'pandas',
        'boto3',
    ],
    author='xtech',
    author_email='support@exponential-tech.ai',
    description='A Python package to interact with the Unifier API.',
    long_description=(Path(__file__).parent / 'README.md').read_text(),
    long_description_content_type='text/markdown',
    url='https://github.com/xtech-analytics/data-unifier',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
    license='GPL-3.0',
)
