from setuptools import setup, find_packages

setup(
    name='celery-s3',
    version='0.1',
    description='An S3 result store backend for Celery',
    long_description=open('README.md').read(),
    author='Kevin Glisson',
    author_email='kevgliss@gmail.com',
    license='BSD',
    url='https://github.com/kevgliss/celery-s3',
    download_url='https://github.com/kevgliss/celery-s3/downloads',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'boto3',
    ],
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'Programming Language :: Python',
        'Operating System :: OS Independent',
    ],
)
