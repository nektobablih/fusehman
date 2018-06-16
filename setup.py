from setuptools import setup, find_packages


setup(
    name='fusehsman',
    version='1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts':
            ['fusehsman = fusehsman.fs:main']
    },
    install_requires=[
        'fusepy>=2.0.4'
    ]
)
