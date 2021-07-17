from setuptools import setup, find_packages, find_namespace_packages


setup(
    name="GameChecker",
    author="Sebastien Cayo",
    author_email="sjc153@case.edu",
    version="0.1.0",
    url = "https://github.com/UVAMobileDev/NcaaGameChecker",
    packages=find_packages(),
    description="MySQL Workbench Table Editing",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    keywords="MySQL, Workbench, NCAA, Basketball, API",
    packages=find_packages(
        where=['']
    ),
    include_package_data=True,
    python_requires=">=3.7",
    install_requires=['beautifulsoup4==4.6.3',
    'bs4==0.0.1',
    'mysql.connector==2.2.9',
    'numpy==1.20.3',
    'pandas==1.1.5',
    'requests==2.19.1',
    'sportsreference==0.5.2',
    'sqlalchemy==1.2.11'],
)
