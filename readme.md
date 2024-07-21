## Usage
We first show the function of some main components in Section `Debug`, then introduce how to deploy the monitor system.
### Debug
#### Client 
The daemon on clients is to periodically collect and aggregate client status information. To view the client status in `JSON`, run 
```Bash
python -m next_cluster.client.client_daemon
```

#### Teamup
To get teamup bookings in `pandas.Dataframe` format, run
```Bash
python -m next_cluster.utils.teamup <teamup_id>
```
where `teamup_id` refers to the ID string in teamup url: `https://teamup.com/<teamup_id>`.

#### Main
The main deemon is to periodically collect node status and booking calendar and aggregrate them into dict.

### Deploy
