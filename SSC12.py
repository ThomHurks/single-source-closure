#! /usr/bin/env python3

__author__ = 'Thom Hurks'
# Implementation of a Hybrid Single Source Closure (SSC12) algorithm, specifically the transitive closure.
# Made for the course Database Technology at the TU/e (University of Technology Eindhoven)
# Algorithms based on the paper "Main Memory Evaluation of Recursive Queries on Multicore Machines"
# by Yang and Zaniolo (University of California, Los Angeles), 2014 IEEE International Conference on Big Data.

# Tested ONLY using Python 3.5 running on Mac OS X Yosemite.

# To implement Boolean Arrays, we used the extra Python bitarray module, version 0.8.1
# URL: https://pypi.python.org/pypi/bitarray/ (Make sure to install this before running the code)

# Input:
# Expects a directed graph in a text file of the form:
# FromNodeId	ToNodeId
# 0 9
# 0 40
# 0 68
# So on each line <number representing from node><tab character><number representing to node>

# The test graphs we use are Kronecker graphs generated using the Stanford Network Analysis Platform (SNAP)
# URL: https://github.com/snap-stanford/snap/tree/master/examples/krongen

import os
import re
import sys
from timeit import default_timer as timer
from array import array
from bitarray import bitarray
import multiprocessing
import argparse
from queue import Full
from fractions import Fraction
from paramiko import *
import pickle

def ParseArgs():
    parser = argparse.ArgumentParser(description='Run the SSC12 algorithm on an input graph')
    output_mutexgroup = parser.add_mutually_exclusive_group()
    output_mutexgroup.add_argument('--overwrite', action='store_true', required=False, help='Overwrite any output files if they already exist.')
    output_mutexgroup.add_argument('--unique', action='store_true', required=False, help='If the output file already exists, find a unique file name.')

    subparsers = parser.add_subparsers(help='List of available commands.', dest='command')
    parser_compute = subparsers.add_parser('compute', help='Read in a plaintext graph or a preprocessed graph, compute the SSC and save the result to disk.')
    parser_preprocess = subparsers.add_parser('preprocess', help='Only invoke the graph preprocessing algorithm and save the result to disk.')

    parser_preprocess.add_argument('inputfile', action='store', type=ExistingFile, help='The text file that the graph will be read from.', metavar='inputfile')
    parser_preprocess.add_argument('graphfile_output', action='store', type=str, help='The file that the preprocessed graph will be written to.', metavar='graphfile')
    parser_preprocess.add_argument('sourcevertices_output', action='store', type=str, help='The file that the discovered source vertices will be written to.', metavar='sourcevertices')
    parser_preprocess.add_argument('--nrofvertexfiles', action='store', required=False, type=int, default=None, help='The source vertices are divided over <nrofvertexfiles> files, using the format <sourcevertices>_nr.extension', metavar='nrofvertexfiles')

    parser_compute.add_argument('outputfile', action='store', type=str, help='The file that the SSC output will be written to.', metavar='outputfile')
    parser_compute.add_argument('--alpha', action='store', required=False, type=Fraction, default=1/8, help='Determines the cutoff point between SSC1 and SSC2.', metavar='alpha')
    parser_compute.add_argument('--beta', action='store', required=False, type=Fraction, default=1/128, help='Determines the cutoff point between SSC1 and SSC2.', metavar='beta')
    parser_compute.add_argument('--pemfile', action='store', required=False, type=ExistingFile, help='The location of the PEM file to use for remote authentication.', metavar='pemfile')

    subparsers_compute = parser_compute.add_subparsers(help='List of available subcommands for computing the SSC.', dest='compute_subcommand')
    subparser_compute_fresh = subparsers_compute.add_parser('fresh', help='Read the input graph, preprocess it, compute the SSC and save the result.')
    subparser_compute_cache = subparsers_compute.add_parser('preprocessed', help='Read in a preprocessed graph, compute the SSC and save the result.')

    subparser_compute_fresh.add_argument('inputfile', action='store', type=ExistingFile, help='The text file that the graph will be read from.', metavar='inputfile')

    subparser_compute_cache.add_argument('graphfile_input', action='store', type=ExistingFile, help='The binary file that the preprocessed graph will be read from.', metavar='graphfile')
    subparser_compute_cache.add_argument('sourcevertices_input', action='store', type=ExistingFile, help='The binary file that the source vertices will be read from.', metavar='sourcevertices')

    return parser.parse_args()


def ExistingFile(filename):
    if os.path.isfile(filename):
        return filename
    else:
        raise argparse.ArgumentTypeError("%s is not a valid input file!" % filename)


def GetValidOutputFilename(outputFilename, overwrite_file, unique):
    outputFile = None
    outputFilenameFinal = ""
    if overwrite_file:
        try:
            outputFile = open(outputFilename, 'w')
            outputFilenameFinal = outputFilename
        except OSError:
            print("Couldn't write to file: %s" % outputFilename)
            exit(1)
    else:
        (outputFilename, extension) = os.path.splitext(outputFilename)
        try:
            outputFile = open(str(outputFilename + extension), 'x')
            outputFilenameFinal = outputFilename + extension
        except FileExistsError:
            print("Output file already exists: %s" % (outputFilename + extension))
            if unique:
                print("Attempting to find a unique file name...")
                (outputFile, outputFilenameFinal) = CreateUniqueOutputfile(outputFilename, extension)
                print("Using the unique file name '%s'" % outputFilenameFinal)
            else:
                exit(1)
    if not outputFile.writable():
        print("Couldn't write to output file '%s'!" % outputFilenameFinal)
        outputFile.close()
        exit(1)
    outputFile.close()
    return outputFilenameFinal


def CreateUniqueOutputfile(outputFilename, extension):
    # Prepare output file
    outputFilenameFinal = ""
    outputFile = None
    outputCreated = False
    attemptCounter = 0
    while not outputCreated:
        try:
            outputFilenameFinal = outputFilename + "_" + str(attemptCounter) + extension
            outputFile = open(outputFilenameFinal, 'x')
            outputCreated = True
        except FileExistsError:
            outputCreated = False
            attemptCounter += 1
    return outputFile, outputFilenameFinal


def ParseInputfile(inputFilename):
    line_re = re.compile("^(?P<nr1>\d+)\t(?P<nr2>\d+)$")
    maxVertexNumber = -1
    adjacentLookup = dict()
    sourceVertices = set()
    targetVertices = set()
    with open(inputFilename) as graphFile:
        # Start reading in the input file
        for line in graphFile:
            lineResult = line_re.match(line)
            if lineResult is not None:
                try:
                    nr1 = int(lineResult.group("nr1"))
                    nr2 = int(lineResult.group("nr2"))
                except ValueError:
                    print("Input graph cannot be parsed!")
                    exit(1)
                sourceVertices.add(nr1)
                targetVertices.add(nr2)
                maxVertexNumber = max(nr1, nr2, maxVertexNumber)
                fromNode = adjacentLookup.get(nr1, set())
                fromNode.add(nr2)
                adjacentLookup[nr1] = fromNode
    uniqueSourceVertices = sourceVertices.difference(targetVertices)
    uniqueTargetVertexCount = len(targetVertices.difference(sourceVertices))
    uniqueVertexCount = len(uniqueSourceVertices) + uniqueTargetVertexCount
    if maxVertexNumber <= 0 or uniqueVertexCount <= 1 or len(sourceVertices) == 0:
        print("Input graph is empty or in the wrong format!")
        exit(1)
    # Since vertex numbers are 0 based and we want to fit the number 0 too.
    maxVertexNumber += 1
    print("Highest Vertex ID: %d" % maxVertexNumber)
    print("Vertex Count: %d" % uniqueVertexCount)
    print("Non-Source Vertices: %d" % uniqueTargetVertexCount)
    print("Source Vertices: %d" % len(uniqueSourceVertices))
    return adjacentLookup, uniqueSourceVertices, uniqueVertexCount, maxVertexNumber


# SSC12 Algorithm (defined in several functions):
def Closure(sourceVertices, adjacentLookup, alpha, beta, nrOfVertices, maxVertexNumber):
    # Setup multiprocessing:
    cpuCount = min(multiprocessing.cpu_count(), len(sourceVertices))
    print("Beginning closure processing with %d parallel threads and thresholds alpha = %d and beta = %d..." %
          (cpuCount, alpha, beta))
    sourceVertexCount = len(sourceVertices)
    closureSet = set()
    vertexQueue = multiprocessing.Queue()
    SSCQueue = multiprocessing.Queue()
    processList = []

    alphaThreshold = nrOfVertices / alpha
    betaThreshold = nrOfVertices / beta
    print(str.format("Thresholds in terms of n: alpha = {0}, beta = {1}, n = {2}", alphaThreshold, betaThreshold, nrOfVertices))

    for _ in range(0, cpuCount):
        processList.append(multiprocessing.Process(target=SSCWorker, args=(vertexQueue, SSCQueue, adjacentLookup,
                                                                           alphaThreshold, betaThreshold, maxVertexNumber),
                                                   daemon=True))
    adderProcess = multiprocessing.Process(target=SourceVertexQueueAdder, args=(sourceVertices, vertexQueue, cpuCount),
                                           daemon=True)

    for process in processList:
        process.start()
    adderProcess.start()

    doneCounter = 0
    while doneCounter < sourceVertexCount:
        ssc = SSCQueue.get()
        closureSet = closureSet.union(ssc)
        doneCounter += 1
        sys.stdout.write("\rProgress: %d out of %d jobs completed." % (doneCounter, sourceVertexCount))
        sys.stdout.flush()
        if adderProcess.exitcode is not None and adderProcess.exitcode != 0:
            print("\nEncountered an error while adding jobs! Job queue was full.")
            exit(1)
    return closureSet


def SourceVertexQueueAdder(sourceVertices, vertexQueue, cpuCount):
    # Prepare multiprocessing jobs:
    queueSize = 0
    try:
        for sourceVertex in sourceVertices:
            vertexQueue.put(sourceVertex, block=True)
            queueSize += 1
        # Insert sentinel values:
        for i in range(0, cpuCount):
            vertexQueue.put(None, block=True)
            queueSize += 1
    except Full:
        vertexQueue.close()
        vertexQueue.cancel_join_thread()
        vertexQueue.join_thread()
        exit(1)


def SSCWorker(vertexQueue, SSCQueue, adjacentLookup, alphaThreshold, betaThreshold, maxVertexNumber):
    thresholdExceeded = False
    vertex = None
    while True:
        vertex = vertexQueue.get(block=True)
        if vertex is not None:
            ssc = SSC1(adjacentLookup, vertex, alphaThreshold, betaThreshold)
            if ssc is not None:
                SSCQueue.put(ssc)
            else:
                thresholdExceeded = True
                print("Thread switched to SSC2.")
                break
        else:
            break
    if thresholdExceeded:
        emptyList = [-1] * maxVertexNumber
        bigDeltaTC = array('i', emptyList)
        smallDeltaTC = array('i', emptyList)
        del emptyList
        d = bitarray(maxVertexNumber)
        SSCQueue.put(SSC2(adjacentLookup, vertex, bigDeltaTC, smallDeltaTC, d, maxVertexNumber))
        while True:
            vertex = vertexQueue.get(block=True)
            if vertex is not None:
                SSCQueue.put(SSC2(adjacentLookup, vertex, bigDeltaTC, smallDeltaTC, d, maxVertexNumber))
            else:
                break


def SSC1(adjacentLookup, sourceVertex, alphaThreshold, betaThreshold):
    tc = set()
    tc.add(sourceVertex)
    bigDeltaTC = set()
    bigDeltaTC.add(sourceVertex)
    while len(bigDeltaTC) != 0:
        costs = ComputeSSC1Cost(adjacentLookup, bigDeltaTC, tc)
        if costs[0] > alphaThreshold or costs[1] > betaThreshold:
            print(str.format("Thresholds violated with C_smallDelta = {0} and C_bigDelta = {1}", costs[0], costs[1]))
            return None
        smallDeltaTC = GetAllAdjacentNodesFromSet(adjacentLookup, bigDeltaTC)
        bigDeltaTC = smallDeltaTC.difference(tc)
        tc = tc.union(bigDeltaTC)
    return tc


def ComputeSSC1Cost(adjacentLookup, bigDeltaTC, tc):
    costSmallDelta = 0
    for vertex in bigDeltaTC:
        adjacent = adjacentLookup.get(vertex, None)
        if adjacent is not None:
            costSmallDelta += len(adjacent)
    costSmallDelta += len(tc) * len(bigDeltaTC)
    # Not sure if the " + |tc''| " part is inside or outside the summation in Fig. 5
    # If it is outside, then use the following line instead of the line before this comment:
    #   costSmallDelta += len(tc)

    costBigDelta = len(tc) + len(bigDeltaTC)
    return (costSmallDelta, costBigDelta)


def SSC2(adjacentLookup, sourceVertex, bigDeltaTC, smallDeltaTC, d, maxVertexNumber):
    tc = set()
    d.setall(False)
    d[sourceVertex] = True
    bigDeltaTC[0] = sourceVertex
    L = 1
    while L != 0:
        l = 0
        for i in range(0, L):
            Z = bigDeltaTC[i]
            # Get all adjacent nodes.
            Z_Adjacent = adjacentLookup.get(Z, set())
            for adjacentNode in Z_Adjacent:
                if not d[adjacentNode]:
                    d[adjacentNode] = True
                    smallDeltaTC[l] = adjacentNode
                    l += 1
        bigDeltaTC = smallDeltaTC[:]
        L = l
    for i in range(0, maxVertexNumber):
        if d[i]:
            tc.add(i)
    return tc


def GetAllAdjacentNodesFromSet(adjacentLookup, inputSet):
    resultSet = set()
    for vertex in inputSet:
        adjacentSet = adjacentLookup.get(vertex, None)
        if adjacentSet is not None and len(adjacentSet) != 0:
            resultSet = resultSet.union(adjacentSet)
    return resultSet


# Part of an experiment to run the SSC12 algorithm across multiple (EC2) instances. Still WIP.
def ExecuteRemoteCommand(command, hostname, pemfile, username='ec2-user'):
    try:
        client = SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(str(hostname),
                       username=str(username),
                       key_filename=str(pemfile))
        stdin, stdout, stderr = client.exec_command(str(command))
        lines = stdout.read().splitlines()
        for line in lines:
            print(line)
    except (BadHostKeyException, AuthenticationException, SSHException, IOError):
        print("Error connecting to instance!")


def WritePreprocessedGraphToFile(adjacentLookup, sourceVertices, vertexCount, maxVertexNumber,
                                 graphFilename, sourceVerticesFilename, overwrite, verticesSplit=None):
    with open(graphFilename, 'w+b') as graphFile:
        pickle.dump((adjacentLookup, vertexCount, maxVertexNumber), graphFile, protocol=pickle.HIGHEST_PROTOCOL)
    sourceVerticesList = list(sourceVertices)
    if verticesSplit is not None and verticesSplit >= 1:
        chunkSize = max(len(sourceVertices) // int(verticesSplit), 1)
        for nr, index in enumerate(range(0, len(sourceVertices), chunkSize)):
            subset = set(sourceVerticesList[index:index+chunkSize])
            (filename, extension) = os.path.splitext(sourceVerticesFilename)
            filename = filename + "_" + str(nr) + extension
            filename = GetValidOutputFilename(filename, overwrite, False)
            with open(filename, 'w+b') as sourceVerticesFile:
                pickle.dump(subset, sourceVerticesFile, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(sourceVerticesFilename, 'w+b') as sourceVerticesFile:
            pickle.dump(sourceVertices, sourceVerticesFile, protocol=pickle.HIGHEST_PROTOCOL)


def ReadPreprocessedGraphFromFile(graphFilename, sourceVerticesFilename):
    with open(graphFilename, 'r+b') as graphFile:
        (adjacentLookup, vertexCount, maxVertexNumber) = pickle.load(graphFile)
    with open(sourceVerticesFilename, 'r+b') as sourceVerticesFile:
        sourceVertices = pickle.load(sourceVerticesFile)
    return adjacentLookup, sourceVertices, vertexCount, maxVertexNumber


def WriteSSCOutputToFile(closure, outputFilename, inputFilename, elapsedTime):
    sortedClosure = sorted(closure)
    print("Elapsed time: " + str(elapsedTime) + " seconds.")
    print("Closure Size: " + str(len(sortedClosure)))
    print("Writing closure output to file...")
    with open(outputFilename, 'w') as outputFile:
        outputFile.write(str.format("# Run of SSC12 on input {0}\n", inputFilename))
        outputFile.write(str.format("# Elapsed time: {0} seconds\n", elapsedTime))
        outputFile.write('"Vertex"\n')
        for vertex in sortedClosure:
            outputFile.write('\"' + str(vertex) + '\"\n')


def Main():
    args = ParseArgs()
    if args.command == 'compute':
        print("Computing the SSC.")
        outputFilename = GetValidOutputFilename(args.outputfile, args.overwrite, args.unique)
        if args.compute_subcommand == 'fresh':
            print("Performing a fresh computation from a text graph input file.")
            inputFilename = args.inputfile
            (adjacentLookup, sourceVertices, vertexCount, maxVertexNumber) = ParseInputfile(args.inputfile)
        elif args.compute_subcommand == 'preprocessed':
            print("Performing a computation on a preprocessed graph input file.")
            inputFilename = args.graphfile_input
            (adjacentLookup, sourceVertices, vertexCount, maxVertexNumber) = ReadPreprocessedGraphFromFile(args.graphfile_input, args.sourcevertices_input)
        else:
            print("Error parsing the compute subcommand from the arguments.")
            exit(1)
        # Call SSC12 algorithm:
        startTime = timer()
        computedClosure = Closure(sourceVertices, adjacentLookup, args.alpha, args.beta, vertexCount, maxVertexNumber)
        endTime = timer()
        WriteSSCOutputToFile(computedClosure, outputFilename, inputFilename, endTime - startTime)
    elif args.command == 'preprocess':
        print("Only preprocessing the graph from a text graph input file.")
        graphfile_output = GetValidOutputFilename(args.graphfile_output, args.overwrite, args.unique)
        sourcevertices_output = GetValidOutputFilename(args.sourcevertices_output, args.overwrite, args.unique)
        (adjacentLookup, sourceVertices, vertexCount, maxVertexNumber) = ParseInputfile(args.inputfile)
        WritePreprocessedGraphToFile(adjacentLookup, sourceVertices, vertexCount, maxVertexNumber,
                                     graphfile_output, sourcevertices_output, args.overwrite, args.nrofvertexfiles)
    else:
        print("Error parsing the command from the arguments.")
        exit(1)
    print("Done!")


if __name__ == "__main__":
    Main()
