import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="learning_orchestra_client",
    version="1.0.3",
    author="Gabriel Ribeiro",
    author_email="gabbriel.rribeiro@gmail.com",
    description="learningOrchestra python client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/learningOrchestra/learningOrchestra-python-client",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
