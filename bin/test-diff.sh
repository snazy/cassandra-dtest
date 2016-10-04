#!/usr/bin/env bash
###############################################################################
# This script runs `nosetests --collect-only` on the branch currently checked
# out and diffs it against the output for the same on master. The purpose here
# is to see what tests were added and removed. As an added benefit, it runs the
# command with NOSE_PASTEABLE set, so if you have nose-pasteable installed, you
# can see the strings to give nosetests in order to run the new tests.
###############################################################################
set -e

# Try to get the current branch
old_ref="`git branch | grep '\*' | cut -d'*' -f2`"
# If we're just looking at a commit, get that commit
if [ "*HEAD detatched*" == "$old_ref" ] ; then
  old_ref="`git log --oneline --format='%H' -1`"
fi
# Get the ref we want to compare it to -- $1 if provided, master by default
compare_ref=${1:-master}

# Do this in a subshell so we can do cleanup after
(
  base_output="`git checkout $compare_ref && NOSE_PASTEABLE=true CASSANDRA_VERSION=git:trunk nosetests --collect-only -vv 2>&1`"
  new_output="`git checkout $old_ref && NOSE_PASTEABLE=true CASSANDRA_VERSION=git:trunk nosetests --collect-only -vv 2>&1`"
  diff <(echo "$base_output") <(echo "$new_output")
) || true
git checkout $old_ref || true
