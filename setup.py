# coding=utf-8

from distutils.core import setup
import mqconnector


def readme():
    with open('README') as f:
        return f.read()


setup(
    name='mqconnector',
    version=mqconnector.__version__,
    packages=['mqconnector', ],
    url='https://github.com/ducminhgd/mqconnector',
    license='GNU General Public License',
    author=mqconnector.__author__,
    author_email=mqconnector.__email__,
    description=mqconnector.__description__,
    long_description=readme(),
    requires={
        'pika': '==0.12.0',
        'requests': '==2.21.0',
    },
    keywords=mqconnector.__keywords__,
    classifiers=[
        'License :: GNU General Public License',
        'Programming Language :: Python',
        'Topic :: Software Development :: Message Queue',
    ],
)
