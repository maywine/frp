# Project Constraints

- The public ingress is a resource-constrained relay node; application services run on a private origin node behind NAT.
- The private origin must initiate every tunnel connection. No inbound Internet port may be required on the origin.
- Traffic between the origin and relay must be encrypted and carried over a web-compatible transport on TCP 443.
- Public services remain separated by domain name. The relay terminates public TLS and maps each domain to a configured tunnel service.
- The preferred transport is one authenticated WSS connection with multiplexed logical streams. Legacy TLS transport remains available until its removal is explicitly approved.
- Certificate ownership belongs to nginx and the certificate automation on the VPS; the WSS tunnel process must not load public TLS private keys.
