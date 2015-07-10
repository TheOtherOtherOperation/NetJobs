#!/usr/bin/env python

# ############################################################################ #
# NetJobs - a network job synchronizer.                                        #
#                                                                              #
# Author: Ramon A. Lovato (ramonalovato.com)                                   #
# For: Deepstorage, LLC (deepstorage.net)                                      #
# Version: 1.0                                                                 #
#                                                                              #
# Usage: NetJobs.py [OPTIONS] [PATH]                                           #
# OPTIONS                                                                      #
#   -h  Display help message.                                                  #
#   -s  Run in simulator mode (disables networking).                           #
#   -v  Run in verbose mode.                                                   #
# PATH                                                                         #
#   Relative or absolute path to configuration file (required).                #
#                                                                              #
# Example: $ NetJobs.py -v "C:\NetJobs\testconfig.txt"                         #
# ############################################################################ #

import sys
import os
import re
import socket
import select
import threading
from collections import deque

# ############################################################################ #
# Constants and global variables.                                              #
# ############################################################################ #
ARGC_MAX = 3
ARGS_REGEX = '\-[hsv]+'
DELIMETER = ': *'
TEST_LABEL_REGEX = '^(\w|\.)+ *:.*$'
TEST_SPEC_REGEX = '^(\w|\.)+ *: *(\d+ *[hms] *: *)?.*\s*$'
TEST_TIMEOUT_REGEX = '^\-timeout *: *((\d+ *[hms])|(none))\s*$$'
TEST_GENERAL_TIMEOUT_REGEX = '^\-generaltimeout *: *((\d+ *[hms])|(none))\s*$'
TEST_MIN_HOSTS_REGEX = '^\-minhosts *: *(\d+|all)\s*$'
TEST_END_REGEX = '^end\s*$'
TIME_FORMAT_REGEX = '\d+ *[hms]'
TIMEOUT_NONE = 0
MIN_HOSTS_ALL = -1
AGENT_LISTEN_PORT = 16192
BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 60
START_STRING = '// START //\n'
KILL_STRING = '// KILL //\n'
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
    path_in = ''
    tests = []
    sockets = {}
    listeners = {}

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
            exit(0)
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

        testLabelRegex = re.compile(TEST_LABEL_REGEX)
        testSpecRegex = re.compile(TEST_SPEC_REGEX)
        testTimeoutRegex = re.compile(TEST_TIMEOUT_REGEX)
        testGeneralTimeoutRegex = re.compile(TEST_GENERAL_TIMEOUT_REGEX)
        testMinHostsRegex = re.compile(TEST_MIN_HOSTS_REGEX)
        testEndRegex = re.compile(TEST_END_REGEX)

        numTests = -1
        
        try:
            with open(self.path_in, 'r', newline='') as file:
                # Filter out empty lines.
                lines = deque(filter(None, (line.rstrip() for line in file)))

                # Read rest of file.
                while lines:
                    inTestBlock = False
                    
                    # Get test name.
                    line = lines.popleft()
                    tokens = re.split(DELIMETER, line)
                    testName = ''
                    if testLabelRegex.match(line) == None:
                        sys.exit('ERROR: file %s: expected test label but found '\
                                 '"%s"' % (self.path_in, line))
                    else:
                        inTestBlock = True
                        target = None
                        timeout = TIMEOUT_NONE
                        generalTimeout = TIMEOUT_NONE
                        minHosts = MIN_HOSTS_ALL
                        testLabel = tokens[0]
                        specs = {}
                        timeouts = {}
                        
                    # Get test specifications. Go until next test label line.
                    while inTestBlock:
                        # Reached end of file without closing block.
                        if not lines:
                            sys.exit('ERROR: file %s: test "%s" - no end marker'
                                     % (self.path_in, testName))
                        line = lines.popleft()
                        tokens = re.split(DELIMETER, line)

                        # Is it a target/spec line?
                        if testSpecRegex.match(line):
                            target = tokens[0]
                            command = tokens[1]
                            # Add test spec to specs dictionary.
                            specs[target] = command
                            # Set timeout to general (might be overridden).
                            timeouts[target] = generalTimeout
                                                        
                        # Is it a timeout line?
                        elif testTimeoutRegex.match(line):
                            try:
                                timeout = evaluate_timeout_string(
                                    tokens[1])
                                if timeout < 0:
                                    raise ValueError
                                else:
                                    if target:
                                        timeouts[target] = timeout
                                    else:
                                        sys.exit('ERROR: file %s: timeout specified '\
                                                 'but no current target'
                                                 % self.path_in)
                            except ValueError:
                                sys.exit('ERROR: file %s: timeout values must be '\
                                         '"none" or integer >= 0'
                                         % self.path_in)

                        # Is it a general timeout line?
                        elif testGeneralTimeoutRegex.match(line):
                            try:
                                generalTimeout = evaluate_timeout_string(
                                    tokens[1])
                                if generalTimeout < 0:
                                    raise ValueError
                            except ValueError:
                                sys.exit('ERROR: file %s: timeout values must be '\
                                         '"none" or integer >= 0'
                                         % self.path_in)
                                
                        # Is it a minhosts line?
                        elif testMinHostsRegex.match(line):
                            if tokens[1] == 'all':
                                minHosts = MIN_HOSTS_ALL
                            else:
                                try:
                                    minHosts = int(tokens[1])
                                except ValueError:
                                    sys.exit('ERROR: file %s: minhosts specification '\
                                         'must be "all" or integer > 0 '
                                         % self.path_in)
                                    
                        # Is it an end marker?
                        elif testEndRegex.match(line):
                            inTestBlock = False
                            # Add the test configuration to the list.
                            self.tests.append(TestConfig(testLabel,
                                                         generalTimeout,
                                                         minHosts,
                                                         specs,
                                                         timeouts))
                            
                        # Else unknown.
                        else:
                            sys.exit('ERROR: file %s: unable to interpret line "%s"'
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
                    sys.exit('ERROR: failed to create socket for target "%s": %s.'
                             % (target, e))
                # Bind socket.
                try:
                    port = AGENT_LISTEN_PORT
                    if test.timeouts[target] == TIMEOUT_NONE:
                        timeout = None
                    else:
                        timeout = test.timeouts[target]
                    sock = socket.create_connection((target, port), timeout=timeout)
                    # Perform a simple echo test to make sure it works.
                    testBytes = bytes('hello, ' + target + '\n', 'UTF-8')
                    sock.sendall(testBytes)
                    response = sock.recv(BUFFER_SIZE)
                    if response != testBytes:
                        sys.exit('ERROR: agent %d failed echo test. Unsure of agent '\
                              'identity. Terminating.' % target)

                    # Timeout.
                    testBytes = bytes('timeout=' + str(test.timeouts[target]) + '\n', 'UTF-8')
                    sock.sendall(testBytes)
                    response = sock.recv(BUFFER_SIZE)
                    if response != testBytes:
                        sys.exit('ERROR: agent %d failed timeout echo test. Terminating.' % target)

                    # Command.
                    testBytes = bytes(test.specs[target] + '\n', 'UTF-8')
                    sock.sendall(testBytes)
                    response = sock.recv(BUFFER_SIZE)
                    if response != testBytes:
                        sys.exit('ERROR: agent %d failed command echo test. Terminating.' % target)

                    # Good to go.
                    self.sockets[target] = sock
                except socket.timeout as e:
                    self.timeoutHandler(e, target, test, self)
                except socket.error as e:
                    sys.exit('ERROR: failed to open connection to socket for target '\
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
            sock = self.sockets[target]
            
            # Start a ListenThread to wait for results.
            listener = ListenThread(target, sock, test.timeouts[target],
                                    self, test)
            self.listeners[target] = listener
            listener.start()

            # Send the start command.
            sock.sendall(bytes(START_STRING, 'UTF-8'))

        if verbose:
            print('        ...finished.\n')

    #
    # Start remote agents.
    #
    def waitForResults(self, test):
        if verbose:
            print('        Waiting for agent results...')

        print()
        print('        -- %s // RESULTS:' % test.label)

        # Listener threads print results here before joining.

        for listener in self.listeners.values():
            listener.join()

        if verbose:
            print('        ...finished.\n')
    
    #
    # Clean up after test.
    #
    def cleanUp(self, test):
        if verbose:
            print('        Cleaning up...')
            
        for sock in list(self.sockets.values()):
            sock.close()

        if verbose:
            print('        ...finished.\n')

    #
    # Timeout handler. Kills all listen threads.
    #
    def timeoutHandler(self, exception, target, test, netJobs):
        "called when a socket timeout occurs"
        if test.minHosts == MIN_HOSTS_ALL:
            print('ERROR: test requires all hosts but host %s timed out.'
                  % target, file=sys.stderr)
            os._exit(1)
        elif test.timeoutsRemaining != None:
            if test.timeoutsRemaining < 1:
                print('ERROR: too many timeouts; test requires at least %d '\
                      'host(s).' % test.minHosts, file=sys.stderr)
                self.stopAndKillListeners()
            else:
                test.results[target] = "TIMEOUT"
                test.timeoutsRemaining -= 1

    #
    # Cause all ListenThreads to rejoin.
    #
    def stopAndKillListeners(self):
        for listener in self.listeners.values():
            listener.kill()
    
    #
    # Start.
    #
    def start(self):
        "begin execution"

        if verbose:
            print('\nStarting run...\n')

        for test in self.tests:
            # Reset instance data structures.
            self.sockets = {}
            self.listeners = {}
                    
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

        if verbose:
            print('\nFinishing...\n')

# ############################################################################ #
# TestConfig class for storing test configurations.                            #
# ############################################################################ #
class TestConfig:
    "data structure class for storing test configurations"

    def __init__(self, label, generalTimeout, minHosts, specs, timeouts):
        "basic initializer"
        self.label = label
        self.generalTimeout = generalTimeout
        self.minHosts = minHosts
        self.specs = specs
        self.timeouts = timeouts
        self.results = {}
        if minHosts == 0:
            self.timeoutsRemaining = None
        else:
            self.timeoutsRemaining = minHosts

# ############################################################################ #
# ListenThread class for listening for test results.                           #
# ############################################################################ #
class ListenThread(threading.Thread):
    "listens for test results for a given agent"

    def __init__(self, target, sock, timeout, netJobs, test):
        threading.Thread.__init__(self)
        self.target = target
        self.sock = sock
        self.timeout = timeout
        self.netJobs = netJobs
        self.test = test

    def run(self):
        if self.timeout == TIMEOUT_NONE:
            self.timeout = None

        running = True
        self.test.results[self.target] = ''
        try:
            while running:
                # Wait for result to be transmitted from agent.
                ready = select.select([self.sock], [], [], self.timeout)
                if ready[0]:
                    buffer = self.sock.recv(BUFFER_SIZE)

                    if buffer:
                        status = buffer.decode('UTF-8').replace('\n', '')
                        self.test.results[self.target] = status
                        running = False
                    else:
                        if self.timeout != None:
                            self.kill()
                            raise socket.timeout()
                else:
                    self.kill()
                    raise socket.timeout
        except socket.timeout as e:
            self.test.results[self.target] = "TIMEOUT"
            self.netJobs.timeoutHandler(e, self.target, self.test, self.netJobs)
        finally:
            self.printResult()

    def kill(self):
        self.running = False
        try:
            self.sock.sendall(bytes(KILL_STRING, 'UTF-8'))
        except:
            pass

    def printResult(self):
        print('            %s : %s' % (self.target, str(self.test.results[self.target])))      

# ############################################################################ #
# Functions.                                                                   #
# ############################################################################ #

#
# Ask the user to provide the config file path.
#
def ask_for_path():
    "ask user to provide config file path"
    
    return input('Please enter the configuration file path: ')

#
# Evaluate timeout string.
#
# Params:
#     timeout String to evaluate.
#
# Return:
#     Timeout specified (in seconds) or 0 if no timeout.
#
def evaluate_timeout_string(timeout):
    "evaluate the passed string to see if it's a valid timeout"

    if timeout == 'none':
        return TIMEOUT_NONE
    else:
        unit = timeout[-1] # Last character in string.
        value = int(timeout[:-1]) # Everything except last character in string.

        if unit is 'h':
            multiplier = 60 * 60
        elif unit is 'm':
            multiplier = 60
        else:
            multiplier = 1
            
        return value * multiplier

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

    # Create NetJobs object to handle the work.
    jobs = NetJobs(sys.argv)

    # Run.
    jobs.start()
            
    # Finish.
    if verbose:
        print('All jobs completed.')
    exit(0)

# ############################################################################ #
# Execute main.                                                                #
# ############################################################################ #
if __name__ == "__main__":
    main()
