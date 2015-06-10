#!/usr/bin/env python
 
from distutils.core import setup, Extension
 
setup(name='magicbox',
 version='0.1',
 description='sdfh',
 author='i',
 author_email='sdf',
 url='sdf',
 packages = ['magicbox',]
)

from magicbox.views import magicbox
#print magicbox
print 
print 'TO START TYPE magicbox AND PRESS ENTER'
while 1:
	x  = raw_input()
	if x=='magicbox':
		print 'Magicbox v0.1.1'
		magicbox()
	else:
	    print 'TO START TYPE magicbox AND PRESS ENTER'


