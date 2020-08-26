from setuptools import setup, find_packages


def load_requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


setup(
    name='api-dog-parser',
    version='2.0',
    packages=find_packages(),
    author='rotten_meat',
    install_requires=load_requirements(),
    url='https://github.com/benhacka/',
    download_url=
    'https://github.com/benhacka/api-dog-json-v2-parser/archive/v_1.0.tar.gz',
    entry_points={
        'console_scripts': [
            'api-dog-pv2=api_dog_parser_v2.parser:main',
        ]
    },
    python_requires='>=3.6')
