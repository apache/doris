# Security Policy

## Supported Versions

It is generally recommended to use the latest release as this project
does not maintain multiple stable branches and providing packages e.g.
for Linux distributions is outside our scope. In particular, versions
before 0.3.18 can be assumed to carry the out-of-bounds-read error in
the LAPACK ?LARRV family of functions that was the subject of 
CVE-2021-4048

## Reporting a Vulnerability

If you suspect that you have found a vulnerability - a defect that could
be abused to compromise the security of a user's code or systems - please
do not use the normal github issue tracker (except perhaps to post a general
warning if you deem that necessary). Instead, please contact the project 
maintainers through the email addresses given in their github user profiles.
Defects found in the "lapack-netlib" subtree should ideally be reported to
the maintainers of the reference implementation of LAPACK, lapack@icl.itk.edu
