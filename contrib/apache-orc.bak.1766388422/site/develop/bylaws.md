---
layout: page
title: Apache ORC Bylaws
---

## Introduction

This document defines the bylaws under which the Apache ORC project
operates. It defines the roles and responsibilities of the project,
who may vote, how voting works, how conflicts are resolved, etc.

ORC is a project of the Apache Software Foundation (ASF) and the foundation
holds the trademark on the name "ORC" and copyright on the combined
code base. The [Apache Foundation
FAQ](https://www.apache.org/foundation/faq.html) and
[How-It-Works](https://www.apache.org/foundation/how-it-works.html)
explain the operation and background of the foundation.

Apache has a [code of
conduct](https://www.apache.org/foundation/policies/conduct.html) that
it expects its members to follow. In particular:

* Be **open and welcoming**. It is important that we grow and
  encourage the community of users and developers for our project.

* Be **collaborative**. Working together on the open mailing lists and
  bug database to make decisions helps the project grow.

* Be **respectful** of others. Everyone is volunteering their time and
  efforts to work on this project. Please be respectful of everyone
  and their views.

ORC is typical of Apache projects in that it operates under a set of
principles, known collectively as the "Apache Way". If you are new to
Apache development, please refer to the [Incubator
project](https://incubator.apache.org/) for more
information on how Apache projects operate.

## Roles and Responsibilities

Apache projects define a set of roles with associated rights and
responsibilities. These roles govern what tasks an individual may
perform within the project. The roles are defined in the following
sections.

### Users

The most important participants in the project are people who use our
software. The majority of our developers start out as users and guide
their development efforts from the user's perspective.  Users
contribute to the Apache projects by providing feedback to developers
in the form of bug reports and feature suggestions. As well, users
participate in the Apache community by helping other users on mailing
lists and user support forums.

### Contributors

Contributors include all of the volunteers who donate time, code,
documentation,
or resources to the ORC Project. A contributor that makes sustained,
welcome contributions to the project may be invited to become a
committer, though the exact timing of such invitations depends on many
factors.

### Committers

The project's committers are responsible for the project's technical
management. Committers have the right to commit to the project's git
repository. Committers may cast binding votes on any technical
discussion.

Committer access is by invitation only and must be approved by
consensus approval of the active Project Management Committee (PMC)
members.

If a committer wishes to leave the project or does not contribute to
the project in any form for six months, the PMC may make them emeritus.
Emeritus committers lose their ability to commit code or cast binding
votes. An emeritus committer may
request reinstatement of commit access from the PMC. Such
reinstatement is subject to consensus approval of active PMC members.

All Apache committers are required to have a signed [Individual
Contributor License
Agreement](https://www.apache.org/licenses/icla.txt) (ICLA) on file
with the Apache Software Foundation. There is a [Committer
FAQ](https://www.apache.org/dev/committers.html) which provides more
details on the requirements for Committers.

A committer who makes a
sustained contribution to the project may be invited to become a
member of the PMC. The form of contribution
is not limited to code. It can also include code review, helping out
users on the mailing lists, documentation, testing, etc.

### Release Manager

A Release Manager (RM) is a committer who volunteers to produce a
Release Candidate. The RM shall publish a Release Plan on the
dev mailing list stating the branch from which they intend to
make a Release Candidate.

### Project Management Committee

The Project Management Committee (PMC) for Apache ORC was created by
the Apache Board in April 2015 when ORC moved out of Hive and became a
top level project at Apache. The PMC is responsible to the board and
the ASF for the management and oversight of the Apache ORC
codebase. The responsibilities of the PMC include

  * Deciding what is distributed as products of the Apache ORC
    project. In particular all releases must be approved by the PMC.

  * Maintaining the project's shared resources, including the codebase
    repository, mailing lists, and websites.

  * Speaking on behalf of the project.

  * Resolving license disputes regarding products of the project

  * Nominating new PMC members and committers

  * Maintaining these bylaws and other guidelines of the project

Membership of the PMC is by invitation only and must be approved by a
consensus approval of active PMC members.

A PMC member is considered
emeritus by their own declaration or by not contributing in any form
to the project for over six months. An emeritus member may request
reinstatement to the PMC. Such reinstatement is subject to consensus
approval of the active PMC members.

The chair of the PMC is appointed by the ASF board. The chair is an
office holder of the Apache Software Foundation (Vice President,
Apache ORC) and has primary responsibility to the board for the
management of the project within the scope of the ORC PMC. The
chair reports to the board quarterly on developments within the ORC
project.

When the project desires a new PMC chair, the PMC votes to recommend a
new chair using [Single Transferable
Vote](https://wiki.apache.org/general/BoardVoting) voting. The decision
must be ratified by the Apache board.

## Decision Making

Within the ORC project, different types of decisions require
different forms of approval. For example, the previous section
describes several decisions which require "consensus approval."
This section defines how voting is performed, the types of
approvals, and which types of decision require which type of approval.

### Voting

Decisions regarding the project are made by votes on the primary
project development mailing list (dev@orc.apache.org). Where
necessary, PMC voting may take place on the private ORC PMC mailing
list. Votes are clearly indicated by subject line starting with
[VOTE]. Votes may contain multiple items for approval and these should
be clearly separated. Voting is carried out by replying to the vote
mail. Voting may take five flavors:

* **+1** -- "Yes," "Agree," or "the action should be performed." In general,
  this vote also indicates a willingness on the behalf of the voter in
  "making it happen."

* **+0** -- This vote indicates a willingness for the action under
  consideration to go ahead. The voter, however, will not be able to
  help.

* **0** -- The voter is neutral on the topic under discussion.

* **-0** -- This vote indicates that the voter does not, in general, agree
   with the proposed action but is not concerned enough to prevent the
   action going ahead.

* **-1** -- This is a negative vote. On issues where consensus is required,
   this vote counts as a veto. All vetoes must contain an explanation
   of why the veto is appropriate. Vetoes with no explanation are
   void. It may also be appropriate for a -1 vote to include an
   alternative course of action.

All participants in the ORC project are encouraged to show their
agreement for or against a particular action by voting, regardless of
whether their vote is binding. Nonbinding votes are useful for
encouraging discussion and understanding the scope of opinions within
the project.

### Approvals

These are the types of approvals that can be sought. Different actions
require different types of approvals.

* **Consensus Approval** -- Consensus approval requires 3 binding +1
  votes and no binding vetoes.

* **Lazy Consensus** -- Lazy consensus requires at least one +1 vote and
  no -1 votes ('silence gives assent').

* **Lazy Majority** -- A lazy majority vote requires 3 binding +1 votes
   and more binding +1 votes than -1 votes.

* **Lazy 2/3 Majority** -- Lazy 2/3 majority votes requires at least 3
  votes and twice as many +1 votes as -1 votes.

### Vetoes

A valid, binding veto cannot be overruled. If a veto is cast, it must
be accompanied by a valid reason explaining the reasons for the
veto. The validity of a veto, if challenged, can be confirmed by
anyone who has a binding vote. This does not necessarily signify
agreement with the veto - merely that the veto is valid.  If you
disagree with a valid veto, you must lobby the person casting the veto
to withdraw their veto. If a veto is not withdrawn, any action that
has already been taken  must be reversed in a timely manner.

### Actions

This section describes the various actions which are undertaken within
the project, the corresponding approval required for that action and
those who have binding votes over the action.

#### Code Change

A change made to a codebase of the project requires *lazy consensus*
of active committers other than the author of the patch. We can commit
changes before they have been reviewed, although we prefer to get
reviews first.

#### Product Release

To make a release, the release manager creates a release candidate and
a vote requiring a *lazy majority* of the active PMC members is
required. Once the vote passes, the release candidate becomes an
official release.

#### Adoption of New Codebase

When the codebase for an existing, released product is to be replaced
with an alternative codebase, it requires a *lazy 2/3 majority* of PMC
members. This also covers the creation of new sub-projects and
submodules within the project.

#### New Committer

When a new committer is proposed for the project, *consensus approval*
of the active PMC members is required.

#### New PMC Member

To promote a committer to a PMC member requires *consensus approval*
of active PMC members.

If the vote passes, the Apache Board must be notified to make the change
official.

#### Committer Removal

Removal of commit privileges requires a *lazy 2/3 majority* of active
PMC members.

#### PMC Member Removal

Removing a PMC member requires a *lazy 2/3 majority* of active PMC
members, excluding the member in question.

If the vote passes, the Apache Board must be notified to make the change
official.

#### Modifying Bylaws

Modifying this document requires a *lazy majority* of active PMC members.

### Voting Timeframes

Votes are open for a minimum period of 72 hours to allow all active
voters time to consider the vote. For holiday weekends or conferences,
consider using a longer vote window. Votes relating to code changes are
not subject to a strict timetable but should be made as timely as
possible.
