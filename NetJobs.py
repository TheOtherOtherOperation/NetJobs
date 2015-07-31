#!/usr/bin/env python

# ############################################################################ #
# NetJobs - a network job synchronizer.                                        #
#                                                                              #
# Author: Ramon A. Lovato (ramonalovato.com)                                   #
# For: Deepstorage, LLC (deepstorage.net)                                      #
# Version: 2.0                                                                 #
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
import time
import datetime
import csv
from collections import deque
from enum import Enum

# ############################################################################ #
# Constants and global variables.                                              #
# ############################################################################ #
ARGC_MAX = 3
ARGS_REGEX = '\-[hsv]+'
FILE_DELIMITER = ': *'
TEST_LABEL_REGEX = '^(\w|\.)+ *: *$'
TEST_SPEC_REGEX = '^(\w|\.)+ *: *(\d+ *[hms] *: *)?.*\s*$'
TEST_TIMEOUT_REGEX = '^\-timeout *: *((\d+ *[hms])|(none))\s*$'
TEST_GENERAL_TIMEOUT_REGEX = '^\-generaltimeout *: *((\d+ *[hms])|(none))\s*$'
TEST_MIN_HOSTS_REGEX = '^\-minhosts *: *(\d+|all)\s*$'
TEST_END_REGEX = '^end\s*$'
TIME_FORMAT_REGEX = '\d+ *[hms]'
TIMEOUT_NONE = 0
MIN_HOSTS_ALL = -1
AGENT_LISTEN_PORT = 16192
BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 60
SELECT_TIMEOUT = 1
SOCKET_DELIMITER = '\t'
READY_STRING = '// READY //\n'
START_STRING = '// START //\n'
KILL_STRING = '// KILL //\n'
DONE_STRING = '// DONE //\n'
SUCCESS_STATUS = 'SUCCESS'
ERROR_STATUS = 'ERROR'
TIMEOUT_STATUS = 'TIMEOUT'
KILLED_STATUS = 'KILLED'

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

    # Flag managed by handle_timeout.
    testAborted = False

    #
    # Initializer.
    #
    def __init__(self, argv):
        "basic initializer"
        # Process CLI arguments.
        self.eval_options(argv)

        if verbose == True:
            print('Setup...')
            print('\t"%s" given as configuration file path.' % (self.path_in))
            
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
    # State machine for parsing the input file.
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

        # Enum for state machine.
        State = Enum('State', 'outsideTest inTestNoTarget inTestAndTarget')
        
        try:
            with open(self.path_in, 'r', newline='') as file:
                # Filter out empty lines and commented lines -- those beginning with a hash ('#').
                # Place remaining lines into a queue.
                lines = deque(filter(lambda line: not line.startswith('#') and line.strip(),
                              (line.strip() for line in file)))
                # Current state.
                state = State.outsideTest

                # Read rest of file.
                while lines:
                    line = lines.popleft()
                    tokens = re.split(FILE_DELIMITER, line)

                    # Outside test block.
                    if state is State.outsideTest:
                        if testLabelRegex.match(line) == None:
                            sys.exit('ERROR: file %s: expected test label but found '\
                                     '"%s"' % (self.path_in, line))
                        else:
                            target = None
                            timeout = TIMEOUT_NONE
                            generalTimeout = TIMEOUT_NONE
                            minHosts = MIN_HOSTS_ALL
                            testLabel = tokens[0]
                            specs = {}
                            timeouts = {}

                            state = State.inTestNoTarget

                    # In test block - no targets specified yet.
                    elif state is State.inTestNoTarget:
                        # Is it a general timeout line?
                        if testGeneralTimeoutRegex.match(line):
                            try:
                                generalTimeout = evaluate_timeout_status(
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
                            sys.exit('ERROR: file %s: test %s contains no targets.'
                                     % (self.path_in, testLabel))

                        # Is it a timeout line?
                        elif testTimeoutRegex.match(line):
                            sys.exit('ERROR: file %s: timeout specified '\
                                                 'but no current target'
                                                 % self.path_in)

                        # Is it a target/spec line?
                        elif testSpecRegex.match(line):
                            state = State.inTestAndTarget
                            # This triggers a fall-through to the inTestAndTarget block.

                        # Else unknown.
                        else:
                            sys.exit('ERROR: file %s: unable to interpret line "%s"'
                                     % (self.path_in, line))
                    
                    #
                    # inTestAndTarget
                    #
                    # Note: deliberately NOT an elif case. If the parser detects a
                    # target/spec line, this block will execute on the same loop iteration.
                    #

                    # In test block and at least one target specified.
                    if state is State.inTestAndTarget:
                        # Is it a target/spec line?
                        if testSpecRegex.match(line):
                            target = tokens[0]
                            command = tokens[1]
                            # Remove start and end quotes (only if both because some commands might already contain quotes).
                            if len(command) > 1 and command.startswith('"') and command.endswith('"'):
                                command = command[1:-1]
                            if not target in specs:
                                specs[target] = []
                            specs[target].append(command)
                            if not target in timeouts:
                                timeouts[target] = {}
                            timeouts[target][command] = generalTimeout

                        # Is it a timeout line? Since at least one test target/spec line must
                        # have been encountered to transition to this state, we just retroactively
                        # apply this timeout to the currently open target and command
                        elif testTimeoutRegex.match(line):
                            try:
                                timeout = evaluate_timeout_status(tokens[1])
                                if timeout < 0:
                                    raise ValueError
                                else:
                                    if not target in timeouts:
                                        timeouts[target] = {}
                                    else:
                                        timeouts[target][command] = timeout
                            except ValueError:
                                sys.exit('ERROR: file %s: timeout values must be '\
                                         '"none" or integer >= 0'
                                         % self.path_in)

                        # Is it an end marker?
                        elif testEndRegex.match(line):
                            state = State.outsideTest
                            # Add the test configuration to the list.
                            self.tests.append(TestConfig(testLabel,
                                                         generalTimeout,
                                                         minHosts,
                                                         specs,
                                                         timeouts))

                        # Is it a general timeout or minhosts line?
                        elif testGeneralTimeoutRegex.match(line) or testMinHostsRegex.match(line):
                            sys.exit('ERROR: file %s: -generalTimeout and -minhosts flags must precede '\
                                     'all target specifications.' % self.path_in)

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
    def prep_agents(self, test):
        if verbose:
            print('\t\tPreparing agents...')

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
                    sock = socket.create_connection((target, port), timeout=SOCKET_TIMEOUT)
                    # Perform a simple echo test to make sure it works.
                    testBytes = bytes('name' + SOCKET_DELIMITER + target + '\n', 'UTF-8')
                    sock.sendall(testBytes)
                    response = sock.recv(BUFFER_SIZE)
                    if response != testBytes:
                        sys.exit('ERROR: agent %s failed echo test. Unsure of agent '\
                                 'identity. Terminating.' % target)

                    # Send commands and timeouts.
                    commands = test.specs[target]
                    timeouts = test.timeouts[target]
                    for command in commands:
                        timeout = timeouts[command]
                        # Command.
                        testBytes = bytes('command' + SOCKET_DELIMITER + command + '\n', 'UTF-8')
                        sock.sendall(testBytes)
                        response = sock.recv(BUFFER_SIZE)
                        if response != testBytes:
                            sys.exit('ERROR: agent %s failed to acknowledge command %s. Terminating.' % (target, test.specs[target]))

                        # Timeout.
                        testBytes = bytes('timeout' + SOCKET_DELIMITER + str(timeout) + '\n', 'UTF-8')
                        sock.sendall(testBytes)
                        response = sock.recv(BUFFER_SIZE)
                        if response != testBytes:
                            sys.exit('ERROR: agent %s failed to acknowledge timeout. Terminating.' % target)

                    # End of commands/timeouts.
                    testBytes = bytes(READY_STRING, 'UTF-8')
                    sock.sendall(testBytes)
                    response = sock.recv(BUFFER_SIZE)
                    if response != testBytes:
                        sys.exit('ERROR: agent %s failed to acknowledge ready. Terminating.' % target)

                    # Good to go.
                    self.sockets[target] = sock
                except socket.timeout:
                    self.handle_timeout(target, test, self)
                except socket.error as e:
                    sys.exit('ERROR: failed to open connection to socket for target '\
                             '"%s": %s.' % (target, e))

        if verbose:
            print('\t\t...finished.\n')

    #
    # Start remote agents.
    #
    def start_agents(self, test):
        if verbose:
            print('\t\tStarting agents...')

        for target in list(self.sockets.keys()):
            sock = self.sockets[target]
            
            # Start a ListenThread to wait for results.
            listener = ListenThread(target, sock, test.listenerTimeouts[target],
                                    self, test)
            self.listeners[target] = listener
            listener.start()

            # Send the start command.
            sock.sendall(bytes(START_STRING, 'UTF-8'))

        if verbose:
            print('\t\t...finished.\n')

    #
    # Start remote agents.
    #
    def wait_for_results(self, test):
        if verbose:
            print('\t\tWaiting for agent results...')

        print()
        print('\t\t-- %s // RESULTS:' % test.label)

        # Listener threads print results here before joining.

        for listener in self.listeners.values():
            listener.join()

        if verbose:
            print('\t\t...finished.\n')
    
    #
    # Clean up after test.
    #
    def clean_up(self, test):
        if verbose:
            print('\t\tCleaning up...')
            
        for sock in list(self.sockets.values()):
            sock.close()

        if verbose:
            print('\t\t...finished.\n')

    #
    # Timeout handler. Kills all listen threads.
    #
    def handle_timeout(self, target, test, netJobs):
        "called when a socket timeout occurs"
        # Makes sure the errors are only printed once.
        if self.testAborted == False:
            if test.minHosts == MIN_HOSTS_ALL:
                self.testAborted = True
                print('\t\tERROR: test requires all hosts but host %s timed out or closed. Aborting.'
                      % target, file=sys.stderr)
                self.stop_and_kill_listeners()
            elif test.timeoutsRemaining != None:
                if test.timeoutsRemaining < 1:
                    self.testAborted = True
                    print('\t\tERROR: too many timeouts; test requires at least %d '\
                          'host(s). Aborting.' % test.minHosts, file=sys.stderr)
                    self.stop_and_kill_listeners()
                else:
                    test.timeoutsRemaining -= 1

    #
    # Cause all ListenThreads to rejoin.
    #
    def stop_and_kill_listeners(self):
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
            # Reset instance variables.
            self.sockets = {}
            self.listeners = {}
            self.testAborted = False
                    
            if verbose:
                print('\t%s...' % test.label)
            # Prepare remote agents.
            self.prep_agents(test)

            # Start remote agents.
            self.start_agents(test)

            # Wait for remote agent return status.
            self.wait_for_results(test)

            # Clean up.
            self.clean_up(test)

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
        
        # Setting up dictionaries.
        self.listenerTimeouts = {}
        for target in specs.keys():
            # Results dictionary.
            self.results[target] = {}
            # Timeouts for the ListenThreads.
            timeout = generalTimeout
            for command in specs[target]:
                # Initialize results contents.
                self.results[target][command] = None
                # Calculate longest timeout - use for thread.
                if timeouts[target][command] == None:
                    timeout = None
                    break
                else:
                    t = int(timeouts[target][command])
                    if t > timeout:
                        timeout = t
            self.listenerTimeouts[target] = timeout

        # Used for log file.
        self.timestamp = datetime.datetime.now().isoformat()

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
        self.running = False

    def run(self):
        if self.timeout == TIMEOUT_NONE:
            self.timeout = None

        self.running = True
        startTime = time.time()
        try:
            while self.running:
                elapsedTime = time.time() - startTime

                # Check for timeout.
                if self.timeout != None and elapsedTime >= self.timeout:
                    self.handle_timeout()
                else:
                    # Wait for result to be transmitted from agent.
                    ready = select.select([self.sock], [], [], SELECT_TIMEOUT)
                    if ready[0]:
                        buffer = self.sock.recv(BUFFER_SIZE)

                        if buffer:
                            status = buffer.decode('UTF-8')
                            if status == DONE_STRING:
                                self.running = False
                                if verbose:
                                    print('\t\t\t\t-- %s reported all jobs complete.' % self.target)
                            else:
                                self.process_result_string(status)
        except:
            self.handle_timeout()
        
        self.print_signoff()

    def handle_timeout(self):
        self.running = False
        if verbose:
            print('\t\t\t\t-- %s timed out before completion of all jobs.' % self.target)
        self.netJobs.handle_timeout(self.target, self.test, self.netJobs)

    def kill(self):
        self.running = False
        try:
            self.sock.sendall(bytes(KILL_STRING, 'UTF-8'))
        except:
            pass

    def process_result_string(self, message):
        tokens = message.split(SOCKET_DELIMITER)
        count = len(tokens)

        # Messages sent to this method should always have 4 tokens each, even
        # if some are empty, but check and pad just in case so we don't go out
        # of bounds.
        if count < 4:
            print('NOTICE: insufficient tokens received by process_results().')
            for i in range(0, 4-count):
                tokens[4-count] = ''
        
        target = tokens[0]
        command = tokens[1]
        status = tokens[2]
        output = tokens[3]

        # Store in test.
        self.test.results[target][command] = (status, output)

        # Print.
        print('\t\t\t%s' % message)

    def print_signoff(self):
        for command in self.test.specs[self.target]:
            if self.test.results[self.target][command] is None:
                self.test.results[self.target][command] = (KILLED_STATUS, '')
                print('\t\t\t' + self.target + SOCKET_DELIMITER + command 
                    + SOCKET_DELIMITER + self.test.results[self.target][command][0]
                    + SOCKET_DELIMITER + self.test.results[self.target][command][1])
        

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
def evaluate_timeout_status(timeout):
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
    print(r'\t-h    Display this message.')
    print(r'\t-s    Run in simulator mode (disables networking).')
    print(r'\t-v    Run in verbose mode.')
    print(r'PATH')
    print(r'\tRelative or absolute path to source file (required).')
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
if __name__ == '__main__':
    main()
