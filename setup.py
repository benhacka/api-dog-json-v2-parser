from setuptools import setup, find_packages


def load_requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


setup(name='api-dog-json-v2-parser',
      version='1.0',
      packages=find_packages(),
      author='rotten_meat',
      install_requires=load_requirements(),
      url='https://github.com/benhacka/api-dog-json-v2-parser/',
      download_url=
      'https://github.com/benhacka/api-dog-json-v2-parser/archive/v1.1.tar.gz',
      entry_points={
          'console_scripts': [
              'api-dog-pv2=api_dog_parser_v2.parser:main',
          ]
      },
      python_requires='>=3.6')
