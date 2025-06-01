import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="quiverquant",
    version="0.1.55",
    author="Chris Kardatzke",
    author_email="chris@quiverquant.com",
    description="Quiver Quantitative Alternative Data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Quiver-Quantitative/python-api",
    py_modules = ["quiverquant"],
    install_requires=[
       'pandas',
       'requests'
    ],
    extras_require={
        'dev': [
            'pytest>=6.0.0',
            'pytest-cov>=2.10.0',
            'python-dotenv>=0.15.0',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
