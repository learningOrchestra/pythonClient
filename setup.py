import setuptools

setuptools.setup(
    name="learning_orchestra_client",
    version="2.0.0",
    author="Gabriel Ribeiro",
    author_email="gabbriel.rribeiro@gmail.com",
    description="learningOrchestra python client",
    url="https://github.com/learningOrchestra/learningOrchestra-python-client",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
