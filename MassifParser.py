__author__ = 'Thom Hurks'

import os
import re

# Regular expressions to parse the input lines.
re_peak = re.compile('Detailed snapshots: \[.*?(?P<peak>\d+) \(peak\).*\]')
re_new = re.compile('Output for algorithm (?P<algo>\d) on dataset (?P<dataset>.+) with (?P<threads>\d{1,2}) threads')

# Generate a new regex dynamically.
def get_re(lineNr):
    return re.compile(str(lineNr) + '(?:\s+[0-9,]+){2}\s+(?P<data>[0-9,]+)(?:\s+[0-9,]+){2}')

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
                    outputFile.write('"Data"\t"Algorithm"\t"Nr Threads"\t"Heap Usage (B)"\n')
                    outputCreated = True
            except FileExistsError:
                attemptCounter += 1
        # Start reading in the input file
        # Parsing state variables:
        correctOutputFound = False
        peakFound = False
        re_line = None
        algo = None
        dataset = None
        threads = None
        for line in massifFile:
            lineCounter += 1
            # Useful content starts after header offset.
            if lineCounter > headerOffset:
                # Basic state machine during parsing:
                if correctOutputFound is False and peakFound is False:
                    newOutput = re_new.search(line)
                    if newOutput is not None:
                        algo = newOutput.group('algo')
                        dataset = newOutput.group('dataset')
                        threads = newOutput.group('threads')
                        if algo is not None and dataset is not None and threads is not None:
                            correctOutputFound = True
                            print('##############################################################################')
                            print(line)
                            continue
                elif correctOutputFound is True and peakFound is False:
                    newPeak = re_peak.search(line)
                    if newPeak is not None:
                        peak = newPeak.group('peak')
                        if peak is not None:
                            peakFound = True
                            re_line = get_re(peak)
                            print(line)
                            continue
                elif correctOutputFound is True and peakFound is True:
                    dataLine = re_line.search(line)
                    if dataLine is not None:
                        memory = dataLine.group('data')
                        if memory is not None:
                            # Reset state.
                            correctOutputFound = False
                            peakFound = False
                            re_line = None
                            print(line)
                            print("data used: " + memory)
                            outputFile.write('\"' + dataset + '\"\t\"' + algo + '\"\t\"' + threads + '\"\t\"' + memory + '\"\n')
                            # Clear data after writing.
                            algo = None
                            dataset = None
                            threads = None
                            memory = None
                else:
                    print("Error on line " + str(lineCounter))
        outputFile.close()
        print("Done parsing " + str(lineCounter) + " lines. Output written to " + outputFileNameFinal)
else:
        print("File does not exist: " + inputFileName)
