from setuptools import setup

if __name__ == "__main__":
    setup(name='rekallpy',
          version='0.2.6',
          description='Spatiotemporal query language',
          url='http://github.com/scanner-research/rekall',
          author='Dan Fu',
          author_email='danfu@cs.stanford.edu',
          license='Apache 2.0',
          packages=['rekall', 'rekall.bounds', 'rekall.stdlib'],
          install_requires=['python-constraint', 'tqdm', 'cloudpickle'],
          setup_requires=['pytest-runner'],
          tests_require=['pytest'],
          zip_safe=False)
