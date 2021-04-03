#!/bin/bash
ip addr add 10.10.10.203/32 dev ens9
ifconfig ens9 mtu 9000
ifconfig ens9 up

# local host and vm
# ping -c 3 10.10.10.101
# ping -c 3 10.10.21.51 # switch vm
ip route add 10.10.10.0/24 dev ens9
#arp -i ens9 -s 10.10.10.221 0c:42:a1:2f:12:be
#arp -i ens9 -s 10.10.10.222 00:22:aa:44:55:68
#arp -i ens9 -s 10.10.10.202 00:22:aa:44:66:03
#arp -i ens9 -s 10.10.10.203 0c:42:a1:41:8a:93
#arp -i ens9 -s 10.10.10.204 0c:42:a1:41:8a:92
arp -i ens9 -s 10.10.10.221 04:3f:72:a2:b7:3a
#arp -i ens9 -s 10.10.10.222 04:3f:72:a2:b7:3a

#arp -i ens9 -s 10.10.10.203 0c:42:a1:41:8a:93
#arp -i ens9 -s 10.10.10.204 00:22:aa:44:66:04
#ping -c 3 10.10.10.221  # switch vm
#ping -c 3 10.10.10.222  # switch vm
#ping -c 3 10.10.10.202  # switch vm
#ping -c 3 10.10.10.203
#ping -c 3 10.10.10.204
ping -c 3 10.10.10.221
#ping -c 3 10.10.10.222
#ping -c 3 10.10.10.203  # switch vm
#ping -c 3 10.10.10.204  # switch vm
# ping -c 3 10.10.10.201  # compute vm (may not be used anymore)

ibv_devinfo
