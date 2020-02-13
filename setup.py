from setuptools import find_packages, setup


with open('README.md') as f:
    long_description = f.read()

PACKAGE_VERSION = '0.1.0'


setup(
    name='liquorice',
    version=PACKAGE_VERSION,
    namespace_packages=['liquorice'],
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=['attrs', 'gino', 'SQLAlchemy'],
    extras_require={
        'testing': ['mypy', 'flake8', 'pytest'],
    },
    description='PostgreSQL-backed generic task queue for async Python',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='MIT',
    author='Artur Ciesielski',
    author_email='artur.ciesielski@gmail.com',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',

        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',

        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities',
    ],
    keywords=['async', 'queue', 'task', 'job', 'postgres'],
    url='https://github.com/ghost-of-turek/liQuorice',
)
