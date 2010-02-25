#!/usr/bin/env python
# encoding: utf-8
"""
runLastz.py

Created by Brant Faircloth on 2010-02-24.
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
import multiprocessing



def interface():
    '''Get the starting parameters from a configuration file'''
    usage = "usage: %prog [options]"

    p = optparse.OptionParser(usage)

    p.add_option('--target', '-t', dest = 'target', action='store', \
type='string', default = None, help='The path to the target file (2bit)', \
metavar='FILE')
    p.add_option('--query', '-q', dest = 'query', action='store', \
type='string', default = None, help='The path to the query file (2bit)', \
metavar='FILE')
    p.add_option('--output', '-o', dest = 'output', action='store', \
type='string', default = None, help='The path to the output file', \
metavar='FILE')
    p.add_option('--nprocs', '-n', dest = 'nprocs', action='store', \
type='int', default = 1, help='The number of processors to use')

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

def q_runner(n_procs, list_item, function, *args):
    '''generic function used to start worker processes'''
    task_queue      = multiprocessing.Queue()
    results_queue   = multiprocessing.JoinableQueue()
    if args:
        arguments = (task_queue, results_queue,) + args
    else:
        arguments = (task_queue, results_queue,)
    results = []
    # reduce processer count if proc count > files
    if len(list_item) < n_procs:
        n_procs = len(list_item)
    for l in list_item:
        task_queue.put(l)
    for _ in range(n_procs):
        p = multiprocessing.Process(target = function, args = arguments).start()
        #print 'Starting %s' % function
    for _ in range(len(list_item)):
        # indicated done results processing
        results.append(results_queue.get()) 
        results_queue.task_done()
    #tell child processes to stop
    for _ in range(n_procs):
        task_queue.put('STOP')
    # join the queue until we're finished processing results
    results_queue.join()
    # not closing the Queues caused me untold heartache and suffering
    task_queue.close()
    results_queue.close()
    return results


def lastzParams(chromo, probe, temp_out):
    cli = \
    '/Users/bcf/src/lastz-distrib-1.02.00-x86_64/src/lastz \
    %s \
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
    --identity=92.5 \
    --coverage=83 \
    --output=%s \
    --format=general-:score,name1,strand1,zstart1,end1,length1,name2,strand2,size2,zstart2,end2,length2,diff,cigar,identity,continuity,coverage' \
    % \
    (chromo, probe, temp_out)
    return cli

def lastz(input, output):
    '''docstring for worker2'''
    for chromo, probe in iter(input.get, 'STOP'):
        print '\t%s' % chromo
        temp_fd, temp_out = tempfile.mkstemp(suffix='.lastz')
        os.close(temp_fd)
        cli = lastzParams(chromo, probe, temp_out)
        lzstdout, lztstderr = subprocess.Popen(cli, shell=True, stdout=subprocess.PIPE, stderr = subprocess.PIPE).communicate(None)
        if lztstderr:
            output.put(lztstderr)
        else:
            output.put(temp_out)

def SingleProcLastz(input, output):
    '''docstring for worker2'''
    #pdb.set_trace()
    chromo, probe = input
    temp_fd, tmp_out = tempfile.mkstemp(suffix='.lastz')
    os.close(temp_fd)
    cli = lastzParams(chromo, probe, temp_out)
    lzstdout, lztstderr = subprocess.Popen(cli, shell=True, stdout=subprocess.PIPE, stderr = subprocess.PIPE).communicate(None)
    if lztstderr:
        output.append(lztstderr)
    else:
        output.append(tmp_out)
    return output


def main():
    start_time      = time.time()
    print 'Started: ', time.strftime("%a %b %d, %Y  %H:%M:%S", time.localtime(start_time))
    options, arg = interface()
    # get individual records from the 2bit file
    chromos =  [os.path.join(options.target, c) for c in bx.seq.twobit.TwoBitFile(file(options.target)).keys()]
    probes = (options.query,) * len(chromos)
    cp = zip(chromos, probes)
    # put those record names on the stack
    print "Running the targets against %s queries..." % len(chromos)
    if options.nprocs == 1:
        results = []
        for each in cp:
            print each
            print results
            results = SingleProcLastz(each, results)
    else:
        results = q_runner(options.nprocs, cp, lastz)
    outp = open(options.output, 'wb')
    print "Writing the results file..."
    for f in results:
        print '\t%s' % f
        # read the file
        outp.write(open(f,'rb').read())
        # cleanup
        os.remove(f)
    outp.close() 
    # stats
    end_time = time.time()
    print 'Ended: ', time.strftime("%a %b %d, %Y  %H:%M:%S", time.localtime(end_time))
    print 'Time for execution: ', (end_time - start_time)/60, 'minutes'

if __name__ == '__main__':
    main()

