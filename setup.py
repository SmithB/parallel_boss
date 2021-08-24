import os
from setuptools import setup, find_packages

# get long_description from README.md
with open("README.md", "r") as fh:
    long_description = fh.read()

# list of all scripts to be included with package
scripts = [os.path.join('scripts',f) for f in os.listdir('scripts')]

setup(
    name='parallel_boss',
    version='1.0.0.0',
    description='python-based parallelization scheme',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='',
    author='Ben Smith', 
    author_email='besmith@uw.edu',
    license='MIT',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Physics',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='parallelization',
    packages=find_packages(),
    scripts=scripts,
)
