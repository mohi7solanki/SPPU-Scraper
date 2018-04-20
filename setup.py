#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = (
    'beautifulsoup4 >= 4.6.0',
    'requests>=2.18.4',
    'PyPDF2>=1.26.0',
    'tablib==0.12.1',
    'lxml >= 4.1.1',
    'openpyxl==2.4.9',
)

setup(
    author="Mohit Solanki",
    author_email='mohitsolanki619@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="A Scraper to fetch SPPU Engineering results.",
    install_requires=requirements,
    license="BSD license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='sppu_scraper, SPPU, Pune University',
    name='SPPU-Scraper',
    packages=find_packages(include=['sppu_scraper']),
    entry_points={
       'console_scripts': ['sppu-scraper=sppu_scraper.main:main'],
    },
    url='https://github.com/mohi7solanki/SPPU-Scraper',
    version='0.1.1',
    zip_safe=False,
)
