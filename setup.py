import os
from setuptools import setup, find_packages

# get long_description from README.md
with open("README.md", "r") as fh:
    long_description = fh.read()

shell_scripts = ['requeue_errors', 'requeue_running', 'run_pworkers', 'worker_20.sh']
shell_scripts = [os.path.join('scripts', f) for f in shell_scripts]

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
    scripts=shell_scripts,
    entry_points={
        'console_scripts': [
            'pboss.py = scripts.pboss:__main__',
            'pworker.py = scripts.pworker:__main__',
            'pdash.py = scripts.pdash:main',
            'log_parser.py = scripts.log_parser:main',
        ],
    },
)
