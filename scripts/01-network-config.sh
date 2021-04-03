#!/bin/bash
ip addr add 10.10.10.201/32 dev ens9
ifconfig ens9 mtu 9000
ifconfig ens9 up

# local host and vm
# ping -c 3 10.10.10.101
# ping -c 3 10.10.21.51 # switch vm
ip route add 10.10.10.0/24 dev ens9
#arp -i ens9 -s 10.10.10.201 04:3f:72:a2:b4:a2
arp -i ens9 -s 10.10.10.202 04:3f:72:a2:b4:a3
arp -i ens9 -s 10.10.10.203 04:3f:72:a2:b5:f2
arp -i ens9 -s 10.10.10.204 04:3f:72:a2:b5:f3
arp -i ens9 -s 10.10.10.205 0c:42:a1:41:8b:5a
arp -i ens9 -s 10.10.10.206 0c:42:a1:41:8b:5b
arp -i ens9 -s 10.10.10.207 04:3f:72:a2:b0:12
arp -i ens9 -s 10.10.10.208 04:3f:72:a2:b0:13
arp -i ens9 -s 10.10.10.221 04:3f:72:a2:b7:3a
arp -i ens9 -s 10.10.10.222 04:3f:72:a2:c5:32

ping -c 3 10.10.10.221  # switch vm
ping -c 3 10.10.10.222  # switch vm
#ping -c 3 10.10.10.201  # switch vm
ping -c 3 10.10.10.202
ping -c 3 10.10.10.203
ping -c 3 10.10.10.204
ping -c 3 10.10.10.205
ping -c 3 10.10.10.206  # switch vm
ping -c 3 10.10.10.207  # switch vm
ping -c 3 10.10.10.208  # compute vm (may not be used anymore)

ibv_devinfo
