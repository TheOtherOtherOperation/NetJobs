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
import shlex
import os

from subprocess import PIPE

# Must match the scheduler constants of the same names, for obvious reasons.
AGENT_LISTEN_PORT = 16192
BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 60
START_STRING = '// START //\n'
KILL_STRING = '// KILL //\n'
SUCCESS_STRING = 'SUCCESS'
ERROR_STRING = 'ERROR'
TIMEOUT_NONE = 0

#
# Main.
#
def main():
    "main function"

    sock = socket.socket()
    listenPort = AGENT_LISTEN_PORT
    sock.bind(('', listenPort))
    sock.listen(1) # Only allow single connection.

    while True:
            print('// NetJobsAgent: listening for scheduler connection on port %d.' \
                  % listenPort)
            print('//     Process blocks indefinitely. Exit with ctrl-C/ctrl-break.\n')
            # Establish connection with client.
            try:
                conn, addr = sock.accept()
            except Exception as e:
                print("ERROR: socket accept failed: %s" % str(e))
                continue
         
            print('Got connection from %s. Communicating on port %s.' \
                  % (addr, listenPort))

            # Set the socket timeout.
            conn.settimeout(SOCKET_TIMEOUT)

            # A simple echo test to make sure the connection works.
            try:
                testBytes = conn.recv(BUFFER_SIZE)
                conn.sendall(testBytes)
                # The name the controller uses to identify this agent.
                myName = testBytes.decode('UTF-8').replace('\n', '').split(' ')[1]
            except Exception as e:
                print("ERROR: echo test failed: %s" % str(e))
                continue

            # Get and echo timeout.
            try:
                testBytes = conn.recv(BUFFER_SIZE)
                conn.sendall(testBytes)
                timeoutString = testBytes.decode('UTF-8').replace('\n', '').split('=')[1]
                if timeoutString == str(TIMEOUT_NONE):
                    timeout = None
                    print("No timeout set.")
                else:
                    timeout = int(timeoutString)
                    print("Timeout set to: %d seconds." % timeout)
            except Exception as e:
                print("ERROR: unable to get timeout: %s" % str(e))
                continue


            # Get and echo command.
            try:
                testBytes = conn.recv(BUFFER_SIZE)
                conn.sendall(testBytes)
                command = testBytes.decode('UTF-8').replace('\n', '')
            except Exception as e:
                print("ERROR: command receipt failed: %s" % str(e))
                continue

            # Remove start and end quotes (only if both because some commands might end with quotes).
            if len(command) > 1 and command.startswith('"') and command.endswith('"'):
                command = command[1:-1]

            print('Command received: %s' % command)

            while True:
                receiveBuffer = conn.recv(BUFFER_SIZE)

                if receiveBuffer:
                    status = receiveBuffer.decode('UTF-8')
                    if status == START_STRING:
                        print('Start command received. Beginning run...')
                        resultString = ''
                        try:
                            args = shlex.split(command)
                            # Spawn the subprocess.
                            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=PIPE)
                            # Create a ListenThread in case client needs to send kill message.
                            sosThread = SOSThread(conn, timeout, proc)
                            sosThread.start()
                            # Blocks until subprocess completes.
                            output, errors = proc.communicate()
                            # Soft stop the SOSThread.
                            sosThread.stop()
                            # Send results back to client.
                            print('Results: ' + str([proc.returncode, errors, output]))
                            if proc.returncode > 0 or errors:
                                resultString = (myName + ' ' + ERROR_STRING + ' '\
                                                + errors.decode('UTF-8'))
                            else:
                                resultString = (myName + ' ' + SUCCESS_STRING + ' '\
                                                + output.decode('UTF-8'))
                        except Exception as e:
                            print('\nERROR: an exception occurred while trying to execute %s: %s\n'\
                                  % (command, str(e)))
                            resultString = (myName + ' ' + ERROR_STRING + ' while trying to execute '\
                                            + command + ': ' + str(e))
                        finally:
                            if len(resultString) > BUFFER_SIZE:
                                resultString = resultString[:BUFFER_SIZE-1]
                            try:
                                conn.sendall(bytes(resultString, 'UTF-8'))
                            except:
                                pass
                        break
                    else:
                        print('ERROR: unknown status received: %s' % status)
                        pass

            # Close the connection.
            conn.close()
            print('Connection closed. Returning to wait mode.\n')

# ############################################################################ #
# SOSThread class for listening for client kill command while running.         #
# ############################################################################ #
class SOSThread(threading.Thread):
    "listens for kill command from client"

    def __init__(self, sock, timeout, proc):
        threading.Thread.__init__(self)
        self.running = False
        self.sock = sock
        self.timeout = timeout
        self.proc = proc

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
                            self.stop_and_kill_process()
                    else:
                        if self.timeout != None:
                            raise socket.timeout
        except:
            self.stop_and_kill_process()

    def stop_and_kill_process(self):
        self.running = False
        try:
            # Kill the subprocess.
            self.proc.terminate()
        except:
            pass
        print('Subprocess killed by remote client.')

    def stop(self):
        self.running = False

# ############################################################################ #
# Execute main.                                                                #
# ############################################################################ #
if __name__ == "__main__":
    main()
