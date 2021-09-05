/**
 * Signal handling
 *
 * License:
 *   This Source Code Form is subject to the terms of
 *   the Mozilla Public License, v. 2.0. If a copy of
 *   the MPL was not distributed with this file, You
 *   can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Authors:
 *   Vladimir Panteleev <vladimir@thecybershadow.net>
 */

module thincow.signals;

import core.sys.posix.signal;

import thincow.common;

__gshared: // disable TLS

extern (C) nothrow @nogc:

// Use the technique described here:
// https://www.gnu.org/software/libc/manual/html_node/Signaling-Yourself.html

private void handleTSTP(int sig)
{
	// TSTP is blocked while operations are running;
	// this should only run while thincow is idle.
	if (globals.dirty)
		globals.dirty = false;

	raise(SIGSTOP);
}

private void singleSigProcMask(int signal, int action)
{
	sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, signal);
	int ret = sigprocmask(action, &set, null);
	if (ret != 0) assert(false, "sigprocmask");
}

void initSignals()
{
	signal(SIGTSTP, &handleTSTP);
	singleSigProcMask(SIGTSTP, SIG_UNBLOCK); // Undo FUSE's blocking
}

/// Sets the dirty flag and blocks TSTP for the lifetime of the return value.
/// Used to wrap execution of block which modify thincow data structures.
auto beginWrite()
{
	struct Unblock
	{
		nothrow @nogc:
		@disable this(this);
		~this()
		{
			singleSigProcMask(SIGTSTP, SIG_UNBLOCK);
		}
	}

	singleSigProcMask(SIGTSTP, SIG_BLOCK);

	if (!globals.dirty)
		globals.dirty = true;

	return Unblock();
}
