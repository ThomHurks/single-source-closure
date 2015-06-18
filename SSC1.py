__author__ = 'Thom Hurks'
# Implementation of a Single Source Closure (SSC1) algorithm, specifically the transitive closure.
# Made for the course Database Technology at the TU/e (University of Technology Eindhoven)
# Algorithms based on the paper "Main Memory Evaluation of Recursive Queries on Multicore Machines"
# by Yang and Zaniolo (University of California, Los Angeles), 2014 IEEE International Conference on Big Data.

# Tested ONLY using Python 3.4.3 running on Mac OS X Yosemite.

import os
import re
import sys
from timeit import default_timer as timer
import multiprocessing

# Use this to set input/output names and output extension.
inputFileName = "../Datasets/kronecker_graph3.txt"
outputFileName = "closure_SSC1_"
outputExtension = ".txt"

# Input:
# Expects a directed graph in a text file of the form:
# FromNodeId	ToNodeId
# 0 9
# 0 40
# 0 68
# So on each line <number representing from node><tab character><number representing to node>

# The test graphs we used were Kronecker graphs generated using the Stanford Network Analysis Platform (SNAP)
# URL: https://github.com/snap-stanford/snap/tree/master/examples/krongen

line_re = re.compile("(?P<nr1>\d+)\t(?P<nr2>\d+)")

lineCounter = 0
# Use this to skip useless headers in the input file.
headerOffset = 4

nodeCount = -1
# Read in the input file.
if os.path.isfile(inputFileName):
    with open(inputFileName) as graphFile:
        # Prepare output file
        outputCreated = False
        attemptCounter = 0
        outputFileNameFinal = ""
        outputFile = None
        while not outputCreated:
            try:
                # The 'w' means open for exclusive creation, failing if the file already exists.
                outputFileNameFinal = outputFileName + str(attemptCounter) + outputExtension
                outputFile = open(outputFileNameFinal, 'x')
                if outputFile.writable():
                    # Write the file header.
                    outputFile.write('"Vertex"\n')
                    outputCreated = True
            except FileExistsError:
                attemptCounter += 1
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
                outputFile.close()
                sys.exit("Input has wrong format!")
        print("Done parsing " + str(lineCounter - headerOffset) + " lines. Output will be written to " + outputFileNameFinal)
else:
        print("File does not exist: " + inputFileName)
        sys.exit("File does not exist: " + inputFileName)

if graph is None or adjacentLookup is None or len(graph) == 0 or len(adjacentLookup) == 0 or nodeCount <= 0:
    if not outputFile.closed:
        outputFile.close()
    sys.exit("Input graph is empty!")
# Since arrays are 0-based.
nodeCount += 1
print("Highest Vertex ID: " + str(nodeCount))
print("Vertex Count: " + str(len(allVertices)))
print("Non-Source Vertices: " + str(len(toVertices)))
sourceVertices = allVertices.difference(toVertices)
print("Source Vertices: " + str(len(sourceVertices)))

# SSC1 Algorithm (defined in 3 functions):
def Closure(sourceVertices, pool, chunkSize):
    closureSet = set()
    for ssc in pool.imap_unordered(SSC1, sourceVertices, chunkSize):
        closureSet = closureSet.union(ssc)
    return closureSet

def SSC1(sourceVertex):
    tc = set()
    tc.add(sourceVertex)
    bigDeltaTC = set()
    bigDeltaTC.add(sourceVertex)
    while len(bigDeltaTC) != 0:
        smallDeltaTC = GetAllAdjacentNodesFromSet(bigDeltaTC)
        bigDeltaTC = smallDeltaTC.difference(tc)
        tc = tc.union(bigDeltaTC)
    return tc

def GetAllAdjacentNodesFromSet(inputSet):
    resultSet = set()
    for vertex in inputSet:
        adjacentSet = adjacentLookup.get(vertex, None)
        if adjacentSet is not None and len(adjacentSet) != 0:
            resultSet = resultSet.union(adjacentSet)
    return resultSet

cpuCount = multiprocessing.cpu_count()
pool = multiprocessing.Pool(cpuCount)
chunkSize = (len(sourceVertices) // cpuCount) // 50
print(str.format("Beginning closure processing with {0} parallel threads and chunk size {1}...", cpuCount, chunkSize))
startTime = timer()
# Call SSC1 algorithm:
computedClosure = Closure(sourceVertices, pool, chunkSize)
endTime = timer()
elapsedTime = endTime - startTime
print("Elapsed time: " + str(elapsedTime) + " seconds.")
print("Closure Size: " + str(len(computedClosure)))
print("Writing closure output to file...")
sortedClosure = sorted(computedClosure)
for vertex in sortedClosure:
    outputFile.write('\"' + str(vertex) + '\"\n')
outputFile.close()
print("Done!")
