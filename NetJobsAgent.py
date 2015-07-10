#!/usr/bin/env python

# ############################################################################ #
# NetJobsAgent - agent server for the NetJobs job synchronizer.                #
#                                                                              #
# Author: Ramon A. Lovato (ramonalovato.com)                                   #
# For: Deepstorage, LLC (deepstorage.net)                                      #
# Version: 1.0                                                                 #
#                                                                              #
# Usage: NetJobsAgent.py                                                       #
#                                                                              #
# Example: $ NetJobsAgent.py                                                   #
# ############################################################################ #

import socket
import select
import subprocess
import signal
import threading
import os
import time

from subprocess import PIPE

# Must match the scheduler constants of the same names, for obvious reasons.
AGENT_LISTEN_PORT = 16192
BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 60
TIMEOUT_NONE = 0
SOCKET_DELIMITER = '\t'
READY_STRING = '// READY //\n'
START_STRING = '// START //\n'
KILL_STRING = '// KILL //\n'
DONE_STRING = '// DONE //\n'
SUCCESS_STATUS = 'SUCCESS'
ERROR_STATUS = 'ERROR'
TIMEOUT_STATUS = 'TIMEOUT'
KILLED_STATUS = 'KILLED'

#
# Get run specifications from remote process.
#
# Params:
#     conn Socket connection to remote process.
#
# Return:
#     List of command strings.
#     List of timeouts.
#
def get_specs(conn):
    global name
    global ready
    global sosTimeout

    sosTimeout = TIMEOUT_NONE

    commands = []
    timeouts = []

    receiveBuffer = ''

    while not ready:
        try:
            receiveBuffer = conn.recv(BUFFER_SIZE)
            conn.sendall(receiveBuffer) # Echo test.
            receiveString = receiveBuffer.decode('UTF-8')
        except Exception as e:
            print("ERROR: an exception occurred while trying to receive specs: %s" % str(e))
            break

        print('\tReceived: "%s".' % receiveString.replace('\n', ''))

        if receiveString == READY_STRING:
            ready = True
            print('\t\t--> Ready string received. Awaiting start message.')
        else:
            tokens = receiveString.replace('\n', '').split(SOCKET_DELIMITER)
            if len(tokens) < 2:
                print('\t\t--> WARNING: invalid message received -- insufficient number of tokens.')
                break
            elif tokens[0] == 'name':
                name = tokens[1]
                print('\t\t--> Registering name: %s.' % tokens[1])
            elif tokens[0] == 'command':
                command = tokens[1]
                commands.append(command)
                print('\t\t--> Registering command: "%s".' % command)
            elif tokens[0] == 'timeout':
                try:
                    timeout = int(tokens[1])
                    if timeout == TIMEOUT_NONE:
                        timeouts.append(None)
                        print('\t\t--> Registering timeout: None.')
                    else:
                        timeouts.append(timeout)
                        print('\t\t--> Registering timeout: %d second(s).' % timeout)
                        # Check if sosTimeout needs to be updated.
                        if timeout > sosTimeout:
                            sosTimeout = timeout
                except ValueError as e:
                    print('ERROR: invalid timeout.')
                    break
            else:
                print('\t\t--> WARNING: unknown message received. Breaking.')
                break

    print() # Blank line.

    return commands, timeouts

#
# Execute the main run.
#
# Params:
#     sock Socket on which we're with communicating client.
#     commands List of commands to execute.
#     timeouts List of timeouts for each command.
#
# Returns:
#     List of subprocess threads.
#
def start_run(sock, commands, timeouts):
    global subthreads

    results = []
    output = []

    print('\n---RESULTS---\n')

    # The lists should be the same length, but just in case, use the shorter one.
    for i in range(0, min(len(commands), len(timeouts))):
        command = commands[i]
        timeout = timeouts[i]

        try:
            proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            print('\nERROR: an exception occurred while trying to spawn the subprocess thread for "%s": %s\n'\
                  % (command, str(e)))
        thread = ProcThread(sock, command, timeout, proc)
        thread.start()
        subthreads.append(thread)

#
# Main.
#
def main():
    "main function"

    global name
    global ready
    global results
    global subthreads

    try:
        listenSock = socket.socket()
        listenPort = AGENT_LISTEN_PORT
        listenSock.bind(('', listenPort))
        listenSock.listen(1) # Only allow single connection.
    except OSError as e:
        sys.exit('CRITICAL ERROR: NetJobsAgent failed to initialize: %s.' % str(e))

    while True:
        print('// NetJobsAgent: listening for scheduler connection on port %d.' \
              % listenPort)
        print('//     Process blocks indefinitely. Exit with ctrl-C/ctrl-break.\n')
        name = ''
        subthreads = []
        results = {}
        ready = False

        # Establish connection with client.
        try:
            sock, addr = listenSock.accept()
        except Exception as e:
            print("ERROR: socket accept failed: %s" % str(e))
            continue
     
        print('Got connection from %s. Communicating on port %s.\n' \
              % (addr, listenPort))

        # Set the socket timeout.
        sock.settimeout(SOCKET_TIMEOUT)

        # Get the run specifications.
        commands, timeouts = get_specs(sock)

        # Spawn the SOSThread.
        sosThread = SOSThread(sock, sosTimeout)

        # Wait for go command.
        while ready:
            receiveBuffer = sock.recv(BUFFER_SIZE)

            if receiveBuffer:
                status = receiveBuffer.decode('UTF-8')
                if status == START_STRING:
                    print('Start command received. Beginning run...')
                    # Start the run.
                    sosThread.start()
                    start_run(sock, commands, timeouts)
                else:
                    print('ERROR: unknown status received: "%s".' % status)
                    pass
                ready = False

        # Block until all subprocesses complete.
        for t in subthreads:
            t.join()

        # Notify client to stop listener thread for this agent.
        try:
            sock.sendall(bytes(DONE_STRING, 'UTF-8'))
        except:
            pass

        # Stop SOSThread
        sosThread.stop()

        # Close the connection.
        try:
            sock.close()
        except:
            pass
        print('\nConnection closed. Returning to wait mode.\n')


# ############################################################################ #
# SOSThread class for listening for client kill command while running.         #
# ############################################################################ #
class SOSThread(threading.Thread):
    "listens for kill command from client"

    def __init__(self, sock, timeout):
        threading.Thread.__init__(self)
        self.running = False
        self.sock = sock
        self.timeout = timeout

    def run(self):
        self.running = True
        try:
            while self.running:
                ready = select.select([self.sock], [], [], self.timeout)
                if ready[0]:
                    buffer = self.sock.recv(BUFFER_SIZE)

                    if buffer:
                        status = buffer.decode('UTF-8')
                        if status == KILL_STRING:
                            print('Run killed by remote client.')
                            self.stop_and_kill_run()
                    else:
                        if self.timeout != None:
                            raise socket.timeout
        except:
            self.timeout_handler()

    def timeout_handler(self):
        self.running = False
        try:
            # Kill all subprocess threads.
            for thread in subthreads:
                thread.stop_and_kill_subproc(TIMEOUT_STATUS + SOCKET_DELIMITER)
        except:
            pass

    def stop_and_kill_run(self):
        self.running = False
        try:
            # Kill all subprocess threads.
            for thread in subthreads:
                thread.stop_and_kill_subproc(KILLED_STATUS + SOCKET_DELIMITER)
        except:
            pass

    def stop(self):
        self.running = False


# ############################################################################ #
# ProcThread class for listening for subprocess completion.                    #
# ############################################################################ #
class ProcThread(threading.Thread):
    "listens for subprocess completion"

    def __init__(self, sock, command, timeout, proc):
        threading.Thread.__init__(self)
        self.running = False
        self.sock = sock
        self.command = command
        self.timeout = timeout
        self.proc = proc
        self.result = 'NONE'

    def run(self):
        self.running = True
        startTime = time.time()
        try:
            while self.running and self.proc.poll() is None: # Checks returncode attribute.
                elapsedTime = time.time() - startTime
                # If timeout exceeded and subprocess is still running.
                if elapsedTime >= self.timeout:
                    self.stop_and_kill_subproc(TIMEOUT_STATUS + SOCKET_DELIMITER)
        except Exception as e:
            print('ERROR: during subprocess execution: %s.' % str(e))
            self.stop_and_kill_subproc(ERROR_STATUS + SOCKET_DELIMITER + str(e))

        self.send_result()

    def send_result(self):
        global results

        if self.result == 'NONE':
            output, errors = self.proc.communicate()
            if self.proc.returncode > 0 or errors:
                self.result = (name + SOCKET_DELIMITER + self.command + SOCKET_DELIMITER
                    + ERROR_STATUS + SOCKET_DELIMITER + errors.decode('UTF-8'))
            else:
                self.result = (name + SOCKET_DELIMITER + self.command + SOCKET_DELIMITER
                    + SUCCESS_STATUS + SOCKET_DELIMITER + output.decode('UTF-8'))

        print('* ' + self.result)

        results[self.command] = self.result

        # Check to make sure we're not overrunning the socket buffer.
        if len(self.result) > BUFFER_SIZE:
            self.result = resultString[:BUFFER_SIZE-1]

        try:
            self.sock.sendall(bytes(self.result, 'UTF-8'))
        except Exception as e:
            print('NOTICE: an exception was caught during transmission of results: %s.'
                % str(e))
            pass

    def stop_and_kill_subproc(self, reason):
        self.running = False
        try:
            # Kill the subprocess.
            self.proc.terminate()
        except:
            pass

        self.result = (name + SOCKET_DELIMITER + self.command + SOCKET_DELIMITER
            + reason)


# ############################################################################ #
# Execute main.                                                                #
# ############################################################################ #
if __name__ == "__main__":
    main()
