from setuptools import setup

if __name__ == "__main__":
    setup(name='rekallpy',
          version='0.3.2',
          description='Compostional video event specification',
          url='http://github.com/scanner-research/rekall',
          author='Dan Fu',
          author_email='danfu@cs.stanford.edu',
          license='Apache 2.0',
          packages=['rekall', 'rekall.bounds', 'rekall.stdlib', 'rekall.tuner'],
          install_requires=['python-constraint', 'tqdm', 'cloudpickle',
                            'urllib3', 'requests'],
          setup_requires=['pytest-runner'],
          tests_require=['pytest'],
          zip_safe=False)
