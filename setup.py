from setuptools import setup, find_packages

setup(
    name='unifier',
    version='0.1.12',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pandas',
    ],
    author='xtech',
    author_email='support@exponential-tech.ai',
    description='A Python package to interact with the Unifier API.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/xtech-analytics/data-unifier',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    license='GPL-3.0',
)
