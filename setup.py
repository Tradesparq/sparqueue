from distutils.core import setup

setup(
    name='sparqueue',
    version='0.2',
    packages=['sparqueue',],
    scripts=[
		'bin/sparqueue-api',
		'bin/sparqueue-cli',
		'bin/sparqueue-worker'
	],
    data_files=[('config', [
            'config/worker.json',
            'config/api.json',
            'config/sparqueue-cli.json'
        ])
    ],
    install_requires=[
        'bottle',
        'redis',
        'sh'
    ],
    license='MIT',
    long_description=open('README.md').read(),
)
