from setuptools import setup

setup(name='bigeye',
      version='0.1',
      description='Monitoring data quality across systems',
      url='https://github.com/BBOXX/BigEye.git',
      author='Hugggsy',
      license='Apache',
      packages=['bigeye'],
      install_requires=[
          'psycopg2-binary',
          'datadog',
          'ruamel.yaml',
          'boto3'
      ],
      zip_safe=False)
