from setuptools import setup

if __name__ == "__main__":
    setup(
        name='rekallpy',
        version='0.1.3',
        description='Spatiotemporal query language',
        url='http://github.com/scanner-research/rekall',
        author='Dan Fu',
        author_email='danfu@stanford.edu',
        license='Apache 2.0',
        packages=['rekall', 'rekall.vgrid_utils'],
        install_requires=['python-constraint', 'tqdm', 'cloudpickle'],
        setup_requires=['pytest-runner'],
        tests_require=['pytest'],
        zip_safe=False)
