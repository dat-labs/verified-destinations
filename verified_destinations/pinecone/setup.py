from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    # "dat-core",
    "dat-core@git+ssh://git@github.com/riju-dc/dat-core@feature/base-classes",
]

TEST_REQUIREMENTS = ["pytest~=8.0"]

setup(
    name="destination_pinecone",
    description="Destination Pinecone",
    author="dat",
    author_email="author@dat-labs.com",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.yml"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
