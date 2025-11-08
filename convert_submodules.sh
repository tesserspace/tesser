#!/bin/bash

# A script to convert all git submodules into regular directories in a monorepo.
# Run this from the root of your repository.

# 1. De-initialize and remove all submodules from Git's tracking.
# The 'git submodule foreach' command will execute the given command in each submodule directory.
# 'git rm --cached "$path"' removes the submodule from the index without deleting the files yet.
# The submodule's configuration is then removed.
git submodule foreach --quiet 'git rm --cached "$path" && git config -f .git/config --remove-section "submodule.$name" && git config -f .gitmodules --remove-section "submodule.$name"'

# 2. Remove the .gitmodules file and the submodule metadata from the .git directory.
# This completes the process of telling Git to stop treating them as submodules.
git rm --cached .gitmodules
rm -rf .git/modules

# 3. Re-add all the submodule code as regular files to the main repository.
# The submodule directories now contain the code, but Git isn't tracking them.
# 'git add .' will stage all these previously untracked files.
echo "Re-adding all submodule code to the main repository..."
git add .

# 4. Commit the changes with a clear message.
echo "Committing the conversion..."
git commit -m "build: convert all submodules to monorepo directories"

echo "✅ All submodules have been successfully converted to regular directories."
echo "You can now push these changes to your remote repository."
