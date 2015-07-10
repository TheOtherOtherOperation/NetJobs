#!/usr/bin/python

# ############################################################################ #
# NetJobs - a network job scheduler.                                           #
#                                                                              #
# Author: Ramon A. Lovato (ramonalovato.com)                                   #
# For: Deepstorage, LLC (deepstorage.net)                                      #
# Version: 0.1                                                                 #
#                                                                              #
# Usage: NetJobs.py [OPTIONS] [PATH]                                           #
# OPTIONS                                                                      #
#   -h  Display help message.                                                  #
#   -s  Run in simulator mode (disables networking).                           #
#   -v  Run in verbose mode.                                                   #
# PATH                                                                         #
#   Relative or absolute path to configuration file (required).                #
#                                                                              #
# Example: NetJobs.py -v "C:\NetJobs\testconfig.txt"                           #
# ############################################################################ #

import sys
import re
import socket
import threading
from collections import deque

# ############################################################################ #
# Constants and global variables.                                              #
# ############################################################################ #
ARGC_MAX = 3
ARGS_REGEX = '\-[hsv]+'
DELIMETER = ': *'
NUM_TESTS_REGEX = '^tests *: *\d+\s*$'
TEST_LABEL_REGEX = '^\w+ *:\s*$'
TEST_SPEC_REGEX = '^(\w|\.)+ *: *.*\s*$'
TEST_TIMEOUT_REGEX = '^\-timeout *: *((\d+)|(none))\s*$'
TEST_MIN_HOSTS_REGEX = '^\-minhosts *: *(\d+|all)\s*$'
TEST_END_REGEX = '^end\s*$'
TIMEOUT_NONE = 0
MIN_HOSTS_ALL = -1
AGENT_LISTEN_PORT = 16192
BUFFER_SIZE = 4096
START_STRING = 'GO'
SUCCESS_STRING = 'SUCCESS'
ERROR_STRING = 'ERROR'

global stdscr
global verbose
verbose = False
global simulate
simulate = False

# ############################################################################ #
# NetJobs class.                                                               #
# ############################################################################ #
class NetJobs:
    "NetJobs main class"

    # Instance variables.
    global verbose
    path_in = ''
    tests = []
    sockets = {}
    listeners = {}
    results = {}

    #
    # Initializer.
    #
    def __init__(self, argv):
        "basic initializer"
        # Process CLI arguments.
        self.eval_options(argv)

        if verbose == True:
            print('Setup...')
            print('    "%s" given as configuration file path.' % (self.path_in))
            
        # Parse configuration file.
        self.parse_config()
        
    #
    # Evaluate CLI arguments.
    #
    def eval_options(self, argv):
        "evaluate CLI arguments and act on them"
        argc = len(argv)
        sample = re.compile(ARGS_REGEX)

        if argc > ARGC_MAX:
            terminate()
        elif argc == 1:
            self.path_in = ask_for_path()
        elif argc == 2:
            if not sample.match(argv[1]) == None:
                self.act_on_options(argv[1])
                self.path_in = ask_for_path()
            else:
                self.path_in = argv[1]
        elif argc == ARGC_MAX:
            if (sample.match(argv[1]) == None or
                not sample.match(argv[2]) == None):
                terminate()
            else:
                self.act_on_options(argv[1])
                self.path_in = argv[2]

    #
    # Process optional CLI arguments. Called as part of eval_options.
    #
    def act_on_options(self, args):
        "act on CLI arguments"
        if 'h' in args:
            instructions()
        if 's' in args:
            global simulate
            simulate = True
        if 'v' in args:
            global verbose
            verbose = True
            print('\nVerbose logging enabled.\n')

    #
    # Parse input file.
    #
    def parse_config(self):
        "configure the run according to the configuration file specifications"

        global verbose

        numTestsRegex = re.compile(NUM_TESTS_REGEX)
        testLabelRegex = re.compile(TEST_LABEL_REGEX)
        testSpecRegex = re.compile(TEST_SPEC_REGEX)
        testTimeoutRegex = re.compile(TEST_TIMEOUT_REGEX)
        testMinHostsRegex = re.compile(TEST_MIN_HOSTS_REGEX)
        testEndRegex = re.compile(TEST_END_REGEX)
        
        numTests = -1
        
        try:
            with open(self.path_in, 'r', newline='') as file:
                # Filter out empty lines.
                lines = deque(filter(None, (line.rstrip() for line in file)))
                
                # Expect first line to give number of tests and timeout type/time.
                line = lines.popleft()
                tokens = line.split()
                if numTestsRegex.match(line) == None:
                    sys.exit('file %s: first line must be of form '\
                             '"tests: x", for x > 0' % self.path_in)
                else:
                    try:
                        numTests = int(tokens[1])
                    except ValueError:
                        sys.exit('file %s: test count must be integer > 0'
                                 % self.path_in)
                        
                if verbose:
                    print('    Test count: %d' % numTests)

                # Dictionary for test specs. Attaches to testConfig object.
                specs = {}

                # Read rest of file.
                while lines:
                    inTestBlock = False
                    
                    # Get test name.
                    line = lines.popleft()
                    tokens = re.split(DELIMETER, line)
                    testName = ''
                    if testLabelRegex.match(line) == None:
                        sys.exit('file %s: expected test label but found '\
                                 '"%s"' % (self.path_in, line))
                    else:
                        inTestBlock = True
                        timeout = TIMEOUT_NONE
                        minHosts = MIN_HOSTS_ALL
                        testLabel = tokens[0]
                        
                    # Get test specifications. Go until next test label line.
                    while inTestBlock:
                        # Reached end of file without closing block.
                        if not lines:
                            sys.exit('file %s: test "%s" - no end marker'
                                     % (self.path_in, testName))
                        line = lines.popleft()
                        tokens = re.split(DELIMETER, line)

                        # Is it a spec line?
                        if testSpecRegex.match(line):
                            target = tokens[0]
                            command = tokens[1]
                            # Add test spec to specs dictionary.
                            specs[target] = command
                        # Is it a timeout line?
                        elif testTimeoutRegex.match(line):
                            if tokens[1] == 'none':
                                timeout = TIMEOUT_NONE
                            else:
                                try:
                                    timeout = int(tokens[1])
                                except ValueError:
                                    sys.exit('file %s: timeout specification '\
                                         'must be "complete" or integer > 0 '\
                                         '(in secs)' % self.path_in)
                        # Is it a minhosts line?
                        elif testMinHostsRegex.match(line):
                            if tokens[1] == 'all':
                                minHosts = MIN_HOSTS_ALL
                            else:
                                try:
                                    minHosts = int(tokens[1])
                                except ValueError:
                                    sys.exit('file %s: minhosts specification '\
                                         'must be "all" or integer > 0 '
                                         % self.path_in)
                        # Is it an end marker?
                        elif testEndRegex.match(line):
                            inTestBlock = False
                            # Add the test configuration to the list.
                            self.tests.append(TestConfig(testLabel, timeout,
                                                         specs, minHosts))
                        # Else unknown.
                        else:
                            sys.exit('file %s: unable to interpret line "%s"'
                                     % (self.path_in, line))
                    
        # Catch IOError exception and exit.
        except IOError as e:
            sys.exit('file %s: %s' % (self.path_in, e))

    #
    # Prepare remote agents.
    #
    def prepAgents(self, test):
        if verbose:
            print('        Preparing agents...')

        targets = list(test.specs.keys())
        for target in targets:
            # Create TCP socket. Skip if in simulation mode.
            if not simulate:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                except socket.error as e:
                    sys.exit('Failed to create socket for target "%s": %s.'
                             % (target, e))
                # Bind socket to local machine name and specified port.
                try:
                    hostname = socket.gethostname()
                    port = AGENT_LISTEN_PORT
                    sock.connect((hostname, port))
                    # Perform a simple echo test to make sure it works.
                    testBytes = bytes('hello, ' + target, 'UTF-8')
                    sock.sendall(testBytes)
                    response = sock.recv(BUFFER_SIZE)
                    if response == testBytes:
                        sock.sendall(bytes(test.specs[target], 'UTF-8'))
                        self.sockets[target] = sock
                    else:
                        # TODO
                        print('Agent %d failed echo test. Skipping.' % target)
                except socket.error as e:
                    sys.exit('Failed to open connection to socket for target '\
                             '"%s": %s.' % (target, e))

        if verbose:
            print('        ...finished.\n')

    #
    # Start remote agents.
    #
    def startAgents(self, test):
        if verbose:
            print('        Starting agents...')

        for target in list(self.sockets.keys()):
            socket = self.sockets[target]
            
            # Start a ListenThread to wait for results.
            listener = ListenThread(target, socket, self.results)
            self.listeners[target] = listener
            listener.start()

            # Send the start command.
            socket.sendall(bytes(START_STRING, 'UTF-8'))

        if verbose:
            print('        ...finished.\n')

    #
    # Start remote agents.
    #
    def waitForResults(self, test):
        if verbose:
            print('        Waiting for agent results...')

        for listener in self.listeners.values():
            listener.join(timeout=None)

        if verbose:
            print('        ...finished.\n')
    
    #
    # Clean up after test.
    #
    def cleanUp(self, test):
        if verbose:
            print('        Cleaning up...')
            
        for socket in list(self.sockets.values()):
            socket.close()

        if verbose:
            print('        ...finished.\n')

    #
    # Print final results.
    #
    def printResults(self, test):
        if verbose:
            print('        Final results...')

        print()

        for target in self.results.keys():
            result = self.results[target]
            print('            %s : %s' % (target, str(result)))

        print()

        if verbose:
            print('        ...finished.\n')

    #
    # Start.
    #
    def start(self):
        "begin execution"

        if verbose:
            print('\nStarting run...\n')

        for test in self.tests:
            if verbose:
                print('    %s...' % test.label)
            # Prepare remote agents.
            self.prepAgents(test)

            # Start remote agents.
            self.startAgents(test)

            # Wait for remote agent return status.
            self.waitForResults(test)

            # Clean up.
            self.cleanUp(test)

            # Print results.
            self.printResults(test)

        if verbose:
            print('\nFinishing...\n')

# ############################################################################ #
# TestConfig class for storing test configurations.                            #
# ############################################################################ #
class TestConfig:
    "data structure class for storing test configurations"

    def __init__(self, label, timeout, specs, minHosts):
        "basic initializer"
        self.label = label
        self.timeout = timeout
        self.specs = specs
        self.minHosts = minHosts

# ############################################################################ #
# ListenThread class for listening for test results.                           #
# ############################################################################ #
class ListenThread(threading.Thread):
    "listens for test results for a given agent"

    def __init__(self, target, socket, results):
        threading.Thread.__init__(self)
        self.target = target
        self.socket = socket
        self.results = results

    def run(self):
        print('run started')
        while True:
            # Wait for result to be transmitted from agent.
            buffer = self.socket.recv(BUFFER_SIZE)

            if buffer:
                status = buffer.decode('UTF-8')
                self.results[self.target] = status
                break
                

# ############################################################################ #
# Functions.                                                                   #
# ############################################################################ #

#
# Ask the user to provide the config file path.
#
def ask_for_path():
    return input('Please enter the configuration file path: ')

#
# Print CLI usage instructions.
#
def instructions():
    "print usage instructions"
    
    print()
    print(r'Usage: NetJobs.py [OPTIONS] [PATH]')
    print(r'OPTIONS')
    print(r'    -h    Display this message.')
    print(r'    -s    Run in simulator mode (disables networking).')
    print(r'    -v    Run in verbose mode.')
    print(r'PATH')
    print(r'    Relative or absolute path to source file (required).')
    print()
    print(r'NetJobs.py -v "C:\NetJobs\testconfig.txt"')
    print()

#
# Exit with error and print instructions.
#
def terminate():
    "terminate with error and print instructions"
    
    instructions()
    sys.exit(1)

#
# Main.
#
def main():
    "main function"
    
    global verbose

    # Create NetJobs object to handle the work.
    jobs = NetJobs(sys.argv)

    # Run.
    jobs.start()
            
    # Finish.
    if verbose:
        print('All jobs completed successfully.')
    exit(0)

# ############################################################################ #
# Execute main.                                                                #
# ############################################################################ #
if __name__ == "__main__":
    main()
