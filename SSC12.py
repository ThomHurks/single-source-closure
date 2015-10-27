__author__ = 'Thom Hurks'
# Implementation of a Hybrid Single Source Closure (SSC12) algorithm, specifically the transitive closure.
# Made for the course Database Technology at the TU/e (University of Technology Eindhoven)
# Algorithms based on the paper "Main Memory Evaluation of Recursive Queries on Multicore Machines"
# by Yang and Zaniolo (University of California, Los Angeles), 2014 IEEE International Conference on Big Data.

# Tested ONLY using Python 3.5 running on Mac OS X Yosemite.

# To implement Boolean Arrays, we used the extra Python bitarray module, version 0.8.1
# URL: https://pypi.python.org/pypi/bitarray/ (Make sure to install this before running the code)

import os
import re
import sys
from timeit import default_timer as timer
from array import array
from bitarray import bitarray
import multiprocessing
import argparse
from queue import Full
from signal import SIGTERM


def ParseArgs():
    parser = argparse.ArgumentParser(description='Run the SSC12 algorithm on an input graph')
    parser.add_argument('inputfile', action='store', type=str, help='The text file that the graph will be read from.', metavar='inputfile')
    parser.add_argument('outputfile', action='store', type=str, help='The file that the CSV output will be written to.', metavar='outputfile')
    parser.add_argument('--overwrite', action='store_true', required=False, help='Overwrite the output file if it already exists.')
    parser.add_argument('--nofail', action='store_true', required=False, help='Overwrite the output file if it already exists.')
    return parser.parse_args()


def GetValidOutputFilename(outputFilename, overwrite_file, nofail):
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
            outputFile = open(outputFilename + extension, 'x')
            outputFilenameFinal = outputFilename + extension
        except FileExistsError:
            print("Output file already exists: %s" % (outputFilename + extension))
            if nofail:
                print("Attempting to find a unique file name...")
                (outputFile, outputFilenameFinal) = CreateUniqueOutputfile(outputFilename, extension)
            else:
                exit(1)
    if not outputFile.writable():
        print("Couldn't write to output file!")
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


args = ParseArgs()
inputFilename = args.inputfile
if not os.path.isfile(inputFilename):
    print("Input file does not exist: %s" % inputFilename)
    exit(1)
outputFilename = GetValidOutputFilename(args.outputfile, args.overwrite, args.nofail)

# Input:
# Expects a directed graph in a text file of the form:
# FromNodeId	ToNodeId
# 0 9
# 0 40
# 0 68
# So on each line <number representing from node><tab character><number representing to node>

# The test graphs we used were Kronecker graphs generated using the Stanford Network Analysis Platform (SNAP)
# URL: https://github.com/snap-stanford/snap/tree/master/examples/krongen

# Determine the cutoff point between SSC1 and SSC2
alpha = 1/8
beta = 1/128

line_re = re.compile("(?P<nr1>\d+)\t(?P<nr2>\d+)")

lineCounter = 0
# Use this to skip useless headers in the input file.
headerOffset = 4

nodeCount = -1
# Read in the input file.
with open(inputFilename) as graphFile:
    # Start reading in the input file
    # Parsing state variables:
    graph = set()
    allVertices = set()
    toVertices = set()
    adjacentLookup = dict()
    errorParsing = False
    for line in graphFile:
        lineCounter += 1
        # Useful content starts after header offset.
        if lineCounter > headerOffset:
            lineResult = line_re.search(line)
            if lineResult is not None:
                nr1 = lineResult.group("nr1")
                nr2 = lineResult.group("nr2")
                if nr1 is not None and nr2 is not None:
                    nr1 = int(nr1)
                    nr2 = int(nr2)
                    nodeCount = max(nr1, nr2, nodeCount)
                    graph.add((nr1, nr2))
                    allVertices.add(nr1)
                    allVertices.add(nr2)
                    toVertices.add(nr2)
                    fromNode = adjacentLookup.get(nr1, None)
                    if fromNode is not None:
                        fromNode.add(nr2)
                    else:
                        fromNode = set()
                        fromNode.add(nr2)
                        adjacentLookup[nr1] = fromNode
                else:
                    errorParsing = True
            else:
                errorParsing = True
        if errorParsing:
            sys.exit("Input has wrong format!")
    print("Done parsing " + str(lineCounter - headerOffset) + " lines. Output will be written to " + outputFilename)

if graph is None or adjacentLookup is None or len(graph) == 0 or len(adjacentLookup) == 0 or nodeCount <= 0:
    print("Input graph is empty!")
    exit(1)
# Since arrays are 0-based.
nodeCount += 1
print("Highest Vertex ID: " + str(nodeCount))
print("Vertex Count: " + str(len(allVertices)))
print("Non-Source Vertices: " + str(len(toVertices)))
sourceVertices = allVertices.difference(toVertices)
if len(sourceVertices) > 0:
    print("Source Vertices: " + str(len(sourceVertices)))
else:
    print("0 Source vertices found!")
    sys.exit("0 Source Vertices found!")


# SSC12 Algorithm (defined in several functions):
def Closure(sourceVertices, alpha, beta, nrOfVertices):
    # Setup multiprocessing:
    cpuCount = min(multiprocessing.cpu_count(), len(sourceVertices))
    print("Beginning closure processing with %d parallel threads and thresholds alpha = %d and beta = %d..." %
          (cpuCount, alpha, beta))
    sourceVertexCount = len(sourceVertices)
    closureSet = set()
    vertexQueue = multiprocessing.Queue()
    SSCQueue = multiprocessing.Queue()
    processList = []
    adderReturnCode = None

    alphaThreshold = nrOfVertices / alpha
    betaThreshold = nrOfVertices / beta
    print(str.format("Thresholds in terms of n: alpha = {0}, beta = {1}, n = {2}", alphaThreshold, betaThreshold, nrOfVertices))

    for _ in range(0, cpuCount):
        processList.append(multiprocessing.Process(target=SSCWorker, args=(vertexQueue, SSCQueue,
                                                                           alphaThreshold, betaThreshold), daemon=True))
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
        print("Queue full at size %d! Sending SIGTERM, goodbye." % queueSize)
        # Exiting the daemon process with an error code is not detected by the parent process.
        # This may be a bug in Python 3.5, so instead using the bruteforce way of sending SIGTERM to process 0.
        os.killpg(0, SIGTERM)


def SSCWorker(vertexQueue, SSCQueue, alphaThreshold, betaThreshold):
    thresholdExceeded = False
    while True:
        vertex = vertexQueue.get(block=True)
        if vertex is not None:
            ssc = SSC1(vertex, alphaThreshold, betaThreshold)
            if ssc is not None:
                SSCQueue.put(ssc)
            else:
                thresholdExceeded = True
                print("Thread switched to SSC2.")
                break
        else:
            break
    if thresholdExceeded:
        emptyList = [-1] * nodeCount
        bigDeltaTC = array('i', emptyList)
        smallDeltaTC = array('i', emptyList)
        del emptyList
        d = bitarray(nodeCount)
        SSCQueue.put(SSC2(vertex, bigDeltaTC, smallDeltaTC, d))
        while True:
            vertex = vertexQueue.get(block=True)
            if vertex is not None:
                SSCQueue.put(SSC2(vertex, bigDeltaTC, smallDeltaTC, d))
            else:
                break


def SSC1(sourceVertex, alphaThreshold, betaThreshold):
    tc = set()
    tc.add(sourceVertex)
    bigDeltaTC = set()
    bigDeltaTC.add(sourceVertex)
    while len(bigDeltaTC) != 0:
        costs = ComputeSSC1Cost(bigDeltaTC, tc)
        if costs[0] > alphaThreshold or costs[1] > betaThreshold:
            print(str.format("Thresholds violated with C_smallDelta = {0} and C_bigDelta = {1}", costs[0], costs[1]))
            return None
        smallDeltaTC = GetAllAdjacentNodesFromSet(bigDeltaTC)
        bigDeltaTC = smallDeltaTC.difference(tc)
        tc = tc.union(bigDeltaTC)
    return tc


def ComputeSSC1Cost(bigDeltaTC, tc):
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


def SSC2(sourceVertex, bigDeltaTC, smallDeltaTC, d):
    tc = set()
    d.setall(False)
    d[sourceVertex] = True
    bigDeltaTC[0] = sourceVertex
    L = 1
    while L != 0:
        l = 0
        for i in range(0, L):
            Z = bigDeltaTC[i]
            Z_Adjacent = GetAllAdjacentNodes(Z)
            for adjacentNode in Z_Adjacent:
                if not d[adjacentNode]:
                    d[adjacentNode] = True
                    smallDeltaTC[l] = adjacentNode
                    l += 1
        bigDeltaTC = smallDeltaTC[:]
        L = l
    for i in range(0, nodeCount):
        if d[i]:
            tc.add(i)
    return tc


def GetAllAdjacentNodes(inputVertex):
    return adjacentLookup.get(inputVertex, set())


def GetAllAdjacentNodesFromSet(inputSet):
    resultSet = set()
    for vertex in inputSet:
        adjacentSet = adjacentLookup.get(vertex, None)
        if adjacentSet is not None and len(adjacentSet) != 0:
            resultSet = resultSet.union(adjacentSet)
    return resultSet


startTime = timer()
# Call SSC12 algorithm:
computedClosure = Closure(sourceVertices, alpha, beta, len(allVertices))
sortedClosure = sorted(computedClosure)
endTime = timer()
elapsedTime = endTime - startTime
print("Elapsed time: " + str(elapsedTime) + " seconds.")
print("Closure Size: " + str(len(computedClosure)))
print("Writing closure output to file...")
with open(outputFilename, 'w') as outputFile:
    outputFile.write(str.format("# Run of SSC12 on input {0}\n", inputFilename))
    outputFile.write(str.format("# Elapsed time: {0} seconds\n", elapsedTime))
    outputFile.write('"Vertex"\n')
    for vertex in sortedClosure:
        outputFile.write('\"' + str(vertex) + '\"\n')
print("Done!")
