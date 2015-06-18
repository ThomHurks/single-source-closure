__author__ = 'Thom Hurks'

import os
import re
import sys
from timeit import default_timer as timer
from array import array
from bitarray import bitarray
import multiprocessing
from functools import partial

# Use this to set input/output names and output extension.
inputFileName = "../Datasets/kronecker_graph3.txt"
outputFileName = "closure_SSC2_"
outputExtension = ".txt"

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

# SSC2 Algorithm (defined in 3 functions):
def Closure(vertexQueue, cpuCount, sourceVertexCount):
    closureSet = set()
    emptyList = [-1] * nodeCount
    SSCQueue = multiprocessing.Queue()
    processList = []
    for _ in range(0, cpuCount):
        processList.append(multiprocessing.Process(target=SSCWorker, args=(vertexQueue, SSCQueue, emptyList), daemon=True))

    for process in processList:
        process.start()

    doneCounter = 0
    while doneCounter < sourceVertexCount:
        ssc = SSCQueue.get()
        closureSet = closureSet.union(ssc)
        doneCounter += 1

    return closureSet

def SSCWorker(vertexQueue, SSCQueue, emptyList):
    bigDeltaTC = array('i', emptyList)
    smallDeltaTC = array('i', emptyList)
    d = bitarray(nodeCount)
    while True:
        vertex = vertexQueue.get()
        if vertex is not None:
            SSCQueue.put(SSC2(vertex, bigDeltaTC, smallDeltaTC, d))
        else:
            break

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

# Setup multiprocessing:
cpuCount = multiprocessing.cpu_count()
vertexQueue = multiprocessing.Queue()
# Prepare multiprocessing jobs:
for sourceVertex in sourceVertices:
    vertexQueue.put(sourceVertex)
# Insert sentinel values:
for i in range(0, cpuCount):
    vertexQueue.put(None)
print(str.format("Beginning closure processing with {0} parallel threads...", cpuCount))
startTime = timer()
# Call SSC2 algorithm:
computedClosure = Closure(vertexQueue, cpuCount, len(sourceVertices))
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
