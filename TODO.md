# NetJobs Planned Features and To-Do List

- Update minhosts behavior.
    - Better clarification between host timeout and task timeout.
    - When minhosts targets return, ping remaining targets to see if they're still responsive. That way, we're not waiting around for hosts that might have potentially timed out.
        - Requires status message support to be added. Control center needs procedure to transmit status request messages and update appropriately. Agents need ability to handle receiving status requests and respond.
- Add periodic updates to control center display, showing elapsed time and number of pending tasks for which we're still waiting and number of hosts we've lost. E.g. (1:15:30) Pending jobs: 2 of 4. Hosts lost: 0 of 2.
- Better abstraction of packet-receipt process in control center. Parts of packet-receipt logic are currently implemented in different locations.