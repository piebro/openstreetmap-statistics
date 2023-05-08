from setuptools import setup
from pathlib import Path

parent_dir = Path(__file__).resolve().parent

setup(
    name="openstreetmapStatistics",
    version="0.0.1",
    description="A python library to create statistics about OpenStreetMap using OSM-Changesets.",
    url="https://github.com/piebro/openstreetmap-statistics/",
    author="Piet Brömmel",
    license="MIT License",
    install_requires=parent_dir.joinpath("requirements.txt").read_text().splitlines(),
    packages=["openstreetmapStatistics"],
)
