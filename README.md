# dash
Dash is a shared filesystem interface that allows clients to offload computation to the storage server, so computation can be pushed closer to the storage server.
The Dash interface assumes that developers provide `annotations` for any programs that can be offloaded to the server.
`annotations` specify a type assignment between the arguments that follow a program, so Dash can figure out which arguments correspond to files that are remote.
The Dash crate includes the server program, client program and backend execution engine necessary to execute programs.

### ISSUES:
* Cleanup
    * Frontend Parser: need to clean up functions that re construct the command
      with the types, especially because all the stuff is effectively done twice
      for type param

