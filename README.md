A project quickly thrown together to find the shortest series of links between two pages (see https://en.wikipedia.org/wiki/Wikipedia:Wiki_Game) - do not expect to find clean code here :D

This project converts an xml file containing every Wikipedia article into an SQLite database containing just the links between them in around 12 minutes. This is then used by the main program to find the shortest path by building a hashtable of paths through Wikipedia
using reference counted strings for RAM efficiency (which, despite this optimisation, can go up to 10GB utility). I have gotten the time down to around 30 microseconds per webpage allowing most paths to be found on the order of seconds despite having to search
millions of references.
