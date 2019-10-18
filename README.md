# dash
Dash is a shared filesystem interface that allows clients to offload computation to the storage server, so computation can be pushed closer to the storage server.
The Dash interface assumes that developers provide `annotations` for any programs that can be offloaded to the server.
`annotations` specify a type assignment between the arguments that follow a program, so Dash can figure out which arguments correspond to files that are remote.
The Dash crate includes the server program, client program and backend execution engine necessary to execute programs.

### ISSUES:
* Frontend
    * How to deal with resolving relative filepaths, when file does not exist yet (you wouldn't be able to call cannonicalize in this case)?

* Backend
    * Unable to pipe stuff from a remote process into a local file
    * How should the tcp ports be setup when multiple processes need to pipe back things to client?
        - It could be multiplexed along a single connection 
            - But that could produce weird ordering issues (in the output)
        - Or multiple connections could be opened
            - That might run into weird port issues if too many ports are being used
