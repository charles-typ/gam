PID=$(pgrep gam)

sudo prlimit --nofile --output RESOURCE,SOFT,HARD --pid $PID
sudo prlimit --nofile=1000000:1000000 --pid $PID
sudo prlimit --nofile --output RESOURCE,SOFT,HARD --pid $PID
