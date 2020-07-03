from setuptools import Extension, setup


hiredis_module = Extension(
    name='_hiredis',
    sources=['fastredis/hiredis.i'],
    include_dirs=['/usr/include/hiredis'],
    libraries=['hiredis'],
)

hiredisb_module = Extension(
    name='_hiredisb',
    sources=['fastredis/hiredisb.i'],
    include_dirs=['/usr/include/hiredis'],
    libraries=['hiredis'],
)

setup(
    name='fastredis',
    version='0.0.0',
    description='https://github.com/kriscode1/fastredis',
    author='Kristofer Christakos',
    author_email='kristofer.christakos@gmail.com',
    #url
    #download_url
    #license
    #platforms
    packages=['fastredis'],
    ext_package='fastredis',
    ext_modules=[hiredis_module, hiredisb_module],
)
