#!/usr/bin/python

# ############################################################################ #
# NetJobsAgent - agent server for the NetJobs job scheduler.                   #
#                                                                              #
# Author: Ramon A. Lovato (ramonalovato.com)                                   #
# For: Deepstorage, LLC (deepstorage.net)                                      #
# Version: 0.1                                                                 #
#                                                                              #
# Usage: NetJobsAgent.py                                                       #
#                                                                              #
# Example: NetJobsAgent.py                                                     #
# ############################################################################ #

import socket
import subprocess
import shlex

from subprocess import PIPE

# Must match the scheduler constants of the same names, for obvious reasons.
AGENT_LISTEN_PORT = 16192
BUFFER_SIZE = 4096
START_STRING = 'GO'
SUCCESS_STRING = 'SUCCESS'
ERROR_STRING = 'ERROR'

#
# Main.
#
def main():
   "main function"

   sock = socket.socket()
   hostname = socket.gethostname() # Gets the local machine name.
   listenPort = AGENT_LISTEN_PORT
   sock.bind((hostname, listenPort))
   sock.listen(1) # Only allow single connection.

   while True:
      print('// NetJobsAgent: listening for scheduler connection on port %d.' \
         % listenPort)
      print('//     Process blocks indefinitely. Exit with ctrl-C/ctrl-break.\n')
      # Establish connection with client.
      conn, addr = sock.accept()
      
      print('Got connection from %s. Communicating on port %s.' \
            % (addr, listenPort))

      # A simple echo test to make sure the connection works.
      echoTestBytes = conn.recv(BUFFER_SIZE)
      conn.sendall(echoTestBytes)
      # The name the controller uses to identify this agent.
      myName = echoTestBytes.decode('UTF-8').split(' ')[1]
      
      command = conn.recv(BUFFER_SIZE).decode('UTF-8')
      print('Command received: %s' % command)

      while True:
         receiveBuffer = conn.recv(BUFFER_SIZE)

         if receiveBuffer:
            status = receiveBuffer.decode('UTF-8')
            if status == START_STRING:
               print('Start command received. Beginning run...')
               args = shlex.split(command)
               proc = subprocess.Popen(args,
                                       stdout=PIPE, stderr=PIPE, shell=True)
               # Blocks until subprocess completes.
               output, errors = proc.communicate()
               print('Results: ' + str([proc.returncode, errors, output]))
               if proc.returncode > 0 or errors:
                  resultString = (myName + ' ' + ERROR_STRING + ' '\
                                  + errors.decode('UTF-8'))
               else:
                  resultString = (myName + ' ' + SUCCESS_STRING + ' '\
                                  + output.decode('UTF-8'))
               conn.sendall(bytes(resultString, 'UTF-8'))
               break
            else:
               print('Unknown status received: %s' % status)

      # Close the connection.
      conn.close()
      print('Connection closed. Returning to wait mode.\n')

# ############################################################################ #
# Execute main.                                                                #
# ############################################################################ #
if __name__ == "__main__":
    main()
