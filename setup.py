from distutils.core import setup

setup(
        name='cityair-api',
        version='0.2.77',
        packages=['cityair_api'],
        url='https://github.com/cityairltd/cityair_python_api',
        license='Apache 2.0',
        author='Egor Korovin',
        author_email='ek@cityair.io',
        description='python api to access data from cityair.io',
        classifiers=['Development Status :: 5 - Production/Stable'],
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as
        # the current state of your package]
        install_requires=[
                'pandas', 'requests', 'progressbar2==3.50.1',
                'cached-property'],
        )
