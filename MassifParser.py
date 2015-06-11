__author__ = 'Thom Hurks'

import os
import re
import sys

# Algorithm IDs:
# 0 is semi-naive
# 1 is smart
# 2 is SSC1
# 3 is SSC2
# 4 is SSC12
algorithmNames = ["SemiNaive", "Smart", "SSC1", "SSC2", "SSC12"]
# Set this to True/False if you want readable algorithm names or just the IDs.
outputAlgorithmNamesReadable = True

# Regular expressions to parse the input lines.
re_peak = re.compile('Detailed snapshots: \[.*?(?P<peak>\d+) \(peak\).*\]')
re_new = re.compile('Output for algorithm (?P<algo>\d) on dataset (?P<dataset>.+) with (?P<threads>\d{1,2}) threads')
re_sample = re.compile('(?P<counter>\d+)(?:\s+[0-9,]+){2}\s+(?P<data>[0-9,]+)(?:\s+[0-9,]+){2}')

# Use this to set input/output names and output extension.
inputFileName = "massif_output.txt"
outputFileName = "massif_parsed"
outputExtension = ".tsv"

lineCounter = 0
# Use this to skip useless headers in the input file.
headerOffset = 0

if os.path.isfile(inputFileName):
    with open(inputFileName) as massifFile:
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
                    outputFile.write('"Data"\t"Algorithm"\t"Nr Threads"\t"Heap Usage (B)"\t"Nr Samples"\n')
                    outputCreated = True
            except FileExistsError:
                attemptCounter += 1
        # Start reading in the input file
        # Parsing state variables:
        algo = None
        dataset = None
        threads = None
        largestSample = -1
        sampleCounter = -1
        for line in massifFile:
            lineCounter += 1
            # Useful content starts after header offset.
            if lineCounter > headerOffset:
                # Basic state machine during parsing:
                newOutput = re_new.search(line)
                if newOutput is not None:
                    temp_algo = newOutput.group('algo')
                    temp_dataset = newOutput.group('dataset')
                    temp_threads = newOutput.group('threads')
                    if temp_algo is not None and temp_dataset is not None and temp_threads is not None:
                        if algo is not None and dataset is not None and threads is not None and largestSample > 0 \
                                and sampleCounter > -1:
                            if outputAlgorithmNamesReadable:
                                algo = algorithmNames[int(algo)]
                            outputFile.write('\"' + dataset + '\"\t\"' + algo + '\"\t\"' + threads + '\"\t\"' +
                                             str(largestSample) + '\"\t\"' + str(sampleCounter) + '\"\n')
                            print(largestSample)
                        algo = temp_algo
                        dataset = temp_dataset
                        threads = temp_threads
                        largestSample = -1
                        sampleCounter = -1
                        print('##############################################################################')
                        print(line)
                        continue
                newSample = re_sample.search(line)
                if newSample is not None:
                    currentCount = newSample.group('counter')
                    dataSample = newSample.group('data')
                    if currentCount is not None and dataSample is not None:
                        currentCount = int(currentCount)
                        dataSample = int(dataSample.replace(',', ''))
                        if currentCount == sampleCounter + 1:
                            if dataSample > largestSample:
                                largestSample = dataSample
                            sampleCounter = currentCount
                            continue
                        else:
                            sys.exit("Samples are not in order!")
        print(largestSample)
        outputFile.close()
        print("Done parsing " + str(lineCounter) + " lines. Output written to " + outputFileNameFinal)
else:
        print("File does not exist: " + inputFileName)
