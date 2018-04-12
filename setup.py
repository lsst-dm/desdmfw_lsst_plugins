import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*")

# The main call
setup(name='desdmfw_lsst_plugins',
      version='0.0.1',
      license="GPL",
      description="Pipeline-agnostic codes for running the LSST pipelines in the DESDM framework",
      author="Michelle Gower",
      author_email="mgower@illinois.edu",
      packages=['desdmfw_lsst_plugins'],
      package_dir={'': 'python'},
      scripts=bin_files,
      data_files=[('ups', ['ups/desdmfw_lsst_plugins.table'])]
      )
