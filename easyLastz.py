#!/usr/bin/env python
# encoding: utf-8

"""
easyLastz.py

Created by Brant Faircloth on 24 March 2010 23:09 PDT (-0700).
Copyright (c) 2010 Brant Faircloth. All rights reserved.
"""

import pdb
import sys
import os
import time
import optparse
import tempfile
import subprocess
import bx.seq.twobit


def interface():
    '''Get the starting parameters from a configuration file'''
    usage = "usage: %prog [options]"
    
    p = optparse.OptionParser(usage)
    
    p.add_option('--target', dest = 'target', action='store', \
type='string', default = None, help='The path to the target file (2bit)', \
metavar='FILE')
    p.add_option('--query', dest = 'query', action='store', \
type='string', default = None, help='The path to the query file (2bit)', \
metavar='FILE')
    p.add_option('--output', dest = 'output', action='store', \
type='string', default = None, help='The path to the output file', \
metavar='FILE')
    p.add_option('--coverage', dest = 'coverage', action='store', \
type='float', default = 83, help='The fraction of bases in the \
entire input sequence (target or query, whichever is shorter) that are \
included in the alignment block, expressed as a percentage')
    p.add_option('--identity', dest = 'identity', action='store', \
type='float', default = 92.5, help='The fraction of aligned bases \
(excluding columns containing gaps or non-ACGT characters) that are \
matches, expressed as a percentage')
    
    (options,arg) = p.parse_args()
    for f in [options.target, options.query, options.output]:
        if not f:
            p.print_help()
            sys.exit(2)
        if f != options.output and not os.path.isfile(f):
            print "You must provide a valid path to the query/target file."
            p.print_help()
            sys.exit(2)
    return options, arg

def lastzParams(target, query, coverage, identity, out):
    cli = \
    'lastz \
    %s[multiple] \
    %s[nameparse=full]\
    --strand=both \
    --seed=12of19 \
    --transition \
    --nogfextend \
    --nochain \
    --gap=400,30 \
    --xdrop=910 \
    --ydrop=8370 \
    --hspthresh=3000 \
    --gappedthresh=3000 \
    --noentropy \
    --coverage=%s \
    --identity=%s \
    --output=%s \
    --format=general-:score,name1,strand1,zstart1,end1,length1,name2,strand2,zstart2,end2,length2,diff,cigar,identity,continuity,coverage' \
    % \
    (target, query, coverage, identity, out)
    return cli

def main():
    start_time      = time.time()
    print 'Started: ', time.strftime("%a %b %d, %Y  %H:%M:%S", time.localtime(start_time))
    options, arg    = interface()
    cli = lastzParams(options.target, options.query, options.coverage, \
        options.identity, options.output)
    lzstdout, lztstderr = subprocess.Popen(cli, shell=True, \
        stdout=subprocess.PIPE, stderr = subprocess.PIPE).communicate(None)
    if lztstderr:
        pdb.set_trace()
    end_time        = time.time()
    print 'Ended: ', time.strftime("%a %b %d, %Y  %H:%M:%S", time.localtime(end_time))
    print 'Time for execution: ', (end_time - start_time)/60, 'minutes'

if __name__ == '__main__':
    main()

