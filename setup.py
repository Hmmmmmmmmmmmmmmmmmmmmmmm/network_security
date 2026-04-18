'''
This file ensures a proper packaging and distributing of
Python project by using setuptools (or distuilts in older
Python ver.) to define the config of your project.
(Such as metadata, dependencies and more)
'''

from setuptools import find_packages, setup
from typing import List

def get_requirements()->List[str]:
    """
    This function returns the list of requirements
    """
    requirements_lst:List[str]=[]
    try:
        with open('requirements.txt','r') as file:
            lines = file.readlines()
            for line in lines:
                requirement = line.strip()
                if requirement and requirement!="-e .":
                    requirements_lst.append(requirement)
    except FileNotFoundError:
        print("requirements.txt file not found!")

    return requirements_lst

setup(
    name = "Network_Security",
    version = "0.0.1",
    author="Aftab Aqueel Khan",
    author_email="aftabaqueelkhan@gmail.com",
    packages=find_packages(),
    install_requires=get_requirements(),
    description="Say Wallahi bro",
)